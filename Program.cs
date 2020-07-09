using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Dropbox.Api;
using Dropbox.Api.Files;
using ICSharpCode.SharpZipLib.Zip;

namespace DropboxStreamUploader
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            try
            {
                var token = args[0];
                var streamUrl = args[1];
                var dropboxDirectory = args[2];
                if (!IsEndingWithSeparator(dropboxDirectory))
                    dropboxDirectory += Path.AltDirectorySeparatorChar;
                string password = args[3];

                var mpegExe = args[4];

                var offlineRecordsDirectory = args[5];

                string reservedFilePath = Path.Combine(offlineRecordsDirectory, "reserved.tmp");

                await DoRecording(null);
                await Task.Delay(-1);

                async Task DoRecording(DateTime? latestCleanup)
                {
                    Process mpegProcess = null;
                    var stopReading = new CancellationTokenSource(); 
                    try
                    {
                        Console.WriteLine("Starting new recording");
                        var mpegStart = new ProcessStartInfo(mpegExe, $"-rtsp_transport tcp -i \"{streamUrl}\" -f matroska -c:v copy -c:a copy - ");
                        mpegStart.UseShellExecute = false;
                        mpegStart.RedirectStandardOutput = true;
                        mpegStart.RedirectStandardInput = true;
                        mpegStart.RedirectStandardError = true;
                        mpegProcess = Process.Start(mpegStart);
                        mpegProcess.Exited += (s, a) => stopReading.Cancel();
                        var mpegSource = new AsyncBufferedReader(mpegProcess.StandardOutput.BaseStream);
                        _ = mpegSource.Start(1024 * 1024 * 10, stopReading.Token)
                            .ContinueWith(t =>
                            {
                                if (t.Exception != null && !(t.Exception is OperationCanceledException))
                                {
                                    Console.WriteLine(t.Exception);
                                }
                            });
                        
                        _ = new AsyncBufferedReader(mpegProcess.StandardError.BaseStream)
                            .Start(1024 * 1024 * 10, stopReading.Token)
                            .ContinueWith(t =>
                            {
                                if (t.Exception != null && !(t.Exception is OperationCanceledException))
                                {
                                    Console.WriteLine(t.Exception);
                                }
                            });


                        var startedAt = Stopwatch.StartNew();
                        var fileName = $"video{DateTime.Now:yyyyMMddHHmm}.zip";
                        var filesToDelete = new HashSet<string>();

                        using (var dropbox = new DropboxClient(token))
                        {
                            if (latestCleanup == null || (DateTime.UtcNow - latestCleanup > TimeSpan.FromHours(1)))
                            {
                                Console.WriteLine("Cleaning up");

                                try
                                {
                                    await dropbox.Files.CreateFolderV2Async(dropboxDirectory.TrimEnd('/'));
                                }
                                catch
                                {
                                }

                                try
                                {

                                    for (var list = await dropbox.Files.ListFolderAsync(dropboxDirectory.TrimEnd('/'), true, limit: 2000);
                                        list != null;
                                        list = list.HasMore ? await dropbox.Files.ListFolderContinueAsync(list.Cursor) : null)
                                    {
                                        foreach (var entry in list.Entries)
                                        {
                                            if (!entry.IsFile) continue;

                                            if (!entry.PathLower.Substring(dropboxDirectory.Length).EndsWith(".zip")) continue;
                                            if ((DateTime.UtcNow - entry.AsFile.ServerModified).TotalHours >= 1)
                                                filesToDelete.Add(entry.PathLower);
                                        }
                                    }

                                    await DeleteFilesBatchAsync();

                                    async Task DeleteFilesBatchAsync()
                                    {
                                        if (filesToDelete.Count > 0)
                                        {
                                            Console.WriteLine($"Deleting files: \n{string.Join("\n", filesToDelete)}");
                                            var j = await dropbox.Files.DeleteBatchAsync(filesToDelete.Select(x => new DeleteArg(x)));
                                            if (j.IsAsyncJobId)
                                            {

                                                for (DeleteBatchJobStatus r = await dropbox.Files.DeleteBatchCheckAsync(j.AsAsyncJobId.Value);
                                                    r.IsInProgress;
                                                    r = await dropbox.Files.DeleteBatchCheckAsync(j.AsAsyncJobId.Value))
                                                {
                                                    await Task.Delay(5000);
                                                }
                                            }

                                            filesToDelete.Clear();
                                        }
                                    }

                                    latestCleanup = DateTime.UtcNow;
                                }
                                catch (Exception e)
                                {
                                    Console.WriteLine("Ignoring cleanup error: " + e);
                                }
                            }
                            
                            if (await mpegProcess.WaitForExitAsync(new CancellationTokenSource(Math.Max(10000 - (int) startedAt.ElapsedMilliseconds, 1)).Token))
                                throw new Exception("ffmpeg terminated abnormally");

                            ZipStrings.UseUnicode = true;
                            ZipStrings.CodePage = 65001;
                            var entryFactory = new ZipEntryFactory();
                            byte[] msBuffer = new byte[1000 * 1000 * 50];
                            int zipBufferSize = 1000 * 1000 * 50;


                            Stopwatch signalledExitAt = null;

                            const int ChunkMinIntervalSeconds = 5;
                            const int ChunkMaxIntervalSeconds = 30;
                            const int ChunkSize = 1024 * 1024 * 2;
                            const int SecondsPerFile = 60;
                            
                            using (var zipWriterUnderlyingStream = new CopyStream())
                            {
                                var bufferStream = new MemoryStream(msBuffer);
                                bufferStream.SetLength(0);

                                UploadSessionStartResult session = null;
                                long offset = 0;

                                string offlineFilePath = Path.Combine(offlineRecordsDirectory, Path.GetFileNameWithoutExtension(fileName) + ".mkv");

                                FileStream CreateOfflineFileStream()
                                {
                                    try
                                    {
                                        // we attempt to overwrite same file again and again so it can't be restored
                                        File.Move(reservedFilePath, offlineFilePath);
                                        var f = new FileStream(offlineFilePath, FileMode.Open, FileAccess.ReadWrite, FileShare.None, zipBufferSize);
                                        f.SetLength(0);
                                        Console.WriteLine("Overwriting ok");
                                        return f;
                                    }
                                    catch
                                    {
                                        Console.WriteLine("Overwriting previous file failed, creating new");
                                        return new FileStream(offlineFilePath, FileMode.Create, FileAccess.ReadWrite, FileShare.None, zipBufferSize);
                                    }
                                }

                                using (var offlineFileWriter = CreateOfflineFileStream())
                                {
                                    using (var zipWriter = new ZipOutputStream(zipWriterUnderlyingStream, zipBufferSize)
                                                           { IsStreamOwner = false, Password = password, UseZip64 = UseZip64.On })
                                    {
                                        try
                                        {
                                            zipWriterUnderlyingStream.CopyTo = bufferStream;
                                            zipWriter.SetLevel(0);
                                            var entry = entryFactory.MakeFileEntry("video.mkv", '/' + "video.mkv", false);
                                            entry.AESKeySize = 256;
                                            zipWriter.PutNextEntry(entry);

                                            async Task ExitCheck()
                                            {
                                                if ((signalledExitAt == null) && (startedAt.Elapsed.TotalSeconds > SecondsPerFile || mpegSource.NewDataLength == 0))
                                                {
                                                    Console.WriteLine("Signaling exit to ffmpeg");
                                                    signalledExitAt = Stopwatch.StartNew();
                                                    _ = DoRecording(latestCleanup);
                                                    await Task.Delay(1000);
                                                    mpegProcess.StandardInput.Write('q');

                                                }
                                                else if (signalledExitAt?.Elapsed.TotalSeconds >= 5 && !mpegProcess.HasExited)
                                                {
                                                    try
                                                    {

                                                        Console.WriteLine("Killing ffmpeg");
                                                        stopReading.Cancel();
                                                        mpegProcess.Kill();
                                                    }
                                                    catch
                                                    {
                                                    }
                                                }

                                            }

                                            var waitingFrom = Stopwatch.StartNew();

                                            while (!mpegProcess.HasExited || mpegSource.NewDataLength > 0)
                                            {
                                                // wait at least this time
                                                await Task.Delay(TimeSpan.FromSeconds(signalledExitAt == null ? ChunkMinIntervalSeconds : 1));

                                                while ((mpegSource.NewDataLength < ChunkSize) && (waitingFrom.Elapsed.TotalSeconds < ChunkMaxIntervalSeconds)
                                                    && !mpegProcess.HasExited
                                                    && signalledExitAt == null)
                                                {
                                                    await Task.Delay(100);
                                                    await ExitCheck();
                                                }
                                                
                                                
                                                int read;
                                                do
                                                {
                                                    await ExitCheck();
                                                    read = mpegSource.Advance();
                                                    if (read == 0) break;

                                                    Console.WriteLine($"Processing {read} bytes of {offlineFilePath}");

                                                    zipWriter.Write(mpegSource.Buffer, 0, read);
                                                    zipWriter.Flush();

                                                    //bufferStream.WriteTo(offlineFileWriter);
                                                    offlineFileWriter.Write(mpegSource.Buffer, 0, read);
                                                    offlineFileWriter.Flush(true);

                                                    bufferStream.Position = 0;
                                                    var length = bufferStream.Length;
                                                    if (session == null)
                                                    {
                                                        session = await AsyncEx.Retry(() =>
                                                        {
                                                            var copy = new MemoryStream(msBuffer, 0, (int) bufferStream.Length);
                                                            return dropbox.Files.UploadSessionStartAsync(new UploadSessionStartArg(), copy);
                                                        });
                                                    }
                                                    else
                                                    {
                                                        await AsyncEx.Retry(() =>
                                                        {
                                                            var copy = new MemoryStream(msBuffer, 0, (int) bufferStream.Length);
                                                            return dropbox.Files.UploadSessionAppendV2Async(new UploadSessionCursor(session.SessionId, (ulong) offset), false,
                                                                copy);
                                                        });
                                                    }

                                                    offset += length;
                                                    zipWriterUnderlyingStream.CopyTo = bufferStream = new MemoryStream(msBuffer);
                                                    bufferStream.SetLength(0);
                                                } while (mpegSource.NewDataLength >= ChunkSize);

                                                waitingFrom.Restart();
                                            }
                                        }
                                        finally
                                        {
                                            // disposing ZipOutputStream causes writing to bufferStream
                                            if (!bufferStream.CanRead && !bufferStream.CanWrite)
                                                zipWriterUnderlyingStream.CopyTo = bufferStream = new MemoryStream(msBuffer);

                                            try
                                            {
                                                bufferStream.SetLength(0);

                                                zipWriter.CloseEntry();
                                                zipWriter.Finish();
                                                zipWriter.Close();

                                                //bufferStream.WriteTo(offlineFileWriter);
                                                //offlineFileWriter.Flush(true);
                                            }
                                            catch
                                            {
                                            }
                                        }
                                    }


                                    if (session != null) // can be null if no data
                                    {
                                        bufferStream.Position = 0;
                                        var commitInfo = new CommitInfo(Path.Combine(dropboxDirectory, fileName),
                                            WriteMode.Overwrite.Instance,
                                            false,
                                            DateTime.UtcNow);

                                        await AsyncEx.Retry(() =>
                                        {
                                            var copy = new MemoryStream(msBuffer, 0, (int) bufferStream.Length);
                                            return dropbox.Files.UploadSessionFinishAsync(new UploadSessionCursor(session.SessionId, (ulong) offset), commitInfo, copy);
                                        });
                                    }


                                    Console.WriteLine("Recording successfully finished, deleting " + offlineFilePath);
                                    //offlineFileWriter.SetLength(0);
                                }

                                try
                                {
                                    //File.Move(offlineFilePath, reservedFilePath);
                                    Console.WriteLine("Successfully marked for overwriting");
                                }
                                catch
                                {
                                    //File.Delete(offlineFilePath);
                                    Console.WriteLine("Can't mark for overwriting, just deleting");
                                }


                            }
                        }
                    }
                    catch (Exception e)
                    {
                        if (mpegProcess?.HasExited == false)
                            mpegProcess.Kill();
                        stopReading.Cancel();
                        // redirecting error to normal output
                        Console.WriteLine(e);
                        Thread.Sleep(TimeSpan.FromMinutes(1));
                        _ = DoRecording(latestCleanup);
                    }
                }
            }
            catch (Exception e)
            {
                // redirecting error to normal output
                Console.WriteLine(e);
                throw;
            }
        }

        static bool IsEndingWithSeparator(string s)
        {
            return (s.Length != 0) && ((s[s.Length - 1] == Path.DirectorySeparatorChar) || (s[s.Length - 1] == Path.AltDirectorySeparatorChar));
        }
    }
}