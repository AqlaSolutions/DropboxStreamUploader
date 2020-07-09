﻿using System;
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

                            if (mpegProcess.WaitForExit(Math.Max(10000 - (int) startedAt.ElapsedMilliseconds, 1)))
                                throw new Exception("ffmpeg terminated abnormally");

                            ZipStrings.UseUnicode = true;
                            ZipStrings.CodePage = 65001;
                            var entryFactory = new ZipEntryFactory();
                            byte[] msBuffer = new byte[1000 * 1000 * 50];
                            int zipBufferSize = 1000 * 1000 * 50;

                            var fileName = "video" + DateTime.Now.ToString("yyyyMMddHHmmssfff") + ".zip";

                            Stopwatch signalledExitAt = null;

                            const int ChunkIntervalSeconds = 35;
                            const int SecondsPerFile = 120;
                            
                            using (var zipWriterUnderlyingStream = new CopyStream())
                            {
                                var bufferStream = new MemoryStream(msBuffer);
                                bufferStream.SetLength(0);

                                UploadSessionStartResult session = null;
                                long offset = 0;

                                string offlineFilePath = Path.Combine(offlineRecordsDirectory, Path.GetFileNameWithoutExtension(fileName) + ".mkv");
                                using (var offlineFileWriter = File.Create(offlineFilePath, zipBufferSize))
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

                                        void SignalExit()
                                        {
                                            Console.WriteLine("Signaling exit to ffmpeg");
                                            signalledExitAt = Stopwatch.StartNew();
                                            _ = DoRecording(latestCleanup);
                                            mpegProcess.StandardInput.Write('q');
                                        }

                                        while (!mpegProcess.HasExited || mpegSource.IsDataAvailable)
                                        {
                                            int read;
                                            await Task.Delay(TimeSpan.FromSeconds(ChunkIntervalSeconds));

                                            if (!mpegSource.IsDataAvailable && !mpegProcess.HasExited && signalledExitAt == null)
                                            {

                                                Console.WriteLine("No data available for " + offlineFilePath);
                                                SignalExit();
                                            }

                                            do
                                            {
                                                read = mpegSource.Advance();
                                                if ((signalledExitAt == null) && (startedAt.Elapsed.TotalSeconds >= SecondsPerFile))
                                                    SignalExit();
                                                else if (signalledExitAt?.Elapsed.TotalSeconds > 10)
                                                    try
                                                    {
                                                        stopReading.Cancel();
                                                        mpegProcess.Kill();
                                                    }
                                                    catch
                                                    {
                                                    }

                                                if (read == 0) break;

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
                                                        var copy = new MemoryStream(msBuffer, 0, (int)bufferStream.Length);
                                                        return dropbox.Files.UploadSessionAppendV2Async(new UploadSessionCursor(session.SessionId, (ulong) offset), false,
                                                            copy);
                                                    });
                                                }

                                                offset += length;
                                                zipWriterUnderlyingStream.CopyTo = bufferStream = new MemoryStream(msBuffer);
                                                bufferStream.SetLength(0);
                                            } while (read >= 1024 * 1024);
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
                                        var copy = new MemoryStream(msBuffer, 0, (int)bufferStream.Length);
                                        return dropbox.Files.UploadSessionFinishAsync(new UploadSessionCursor(session.SessionId, (ulong) offset), commitInfo, copy);
                                    });
                                }


                                Console.WriteLine("Recording successfully finished, deleting " + offlineFilePath);

                                File.Delete(offlineFilePath);


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