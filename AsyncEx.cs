using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace DropboxStreamUploader
{
    public static class AsyncEx
    {
        public static async Task<T> Retry<T>(Func<Task<T>> t)
        {
            for (int pause = 1000; pause < 60000; pause += pause + 1000)
            {
                try
                {
                    return await t();
                }
                catch
                {
                    await Task.Delay(pause);
                }
            }

            return await t();
        }

        public static Task Retry(Func<Task> t)
        {
            return Retry(async () =>
            {
                await t();
                return true;
            });
        }

        // https://stackoverflow.com/questions/470256/process-waitforexit-asynchronously
        /// <summary>
        /// Waits asynchronously for the process to exit.
        /// </summary>
        /// <param name="process">The process to wait for cancellation.</param>
        /// <param name="cancellationToken">A cancellation token. If invoked, the task will return 
        /// immediately as canceled.</param>
        /// <returns>A Task representing waiting for the process to end.</returns>
        public static Task<bool> WaitForExitAsync(this Process process,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (process.HasExited) return Task.FromResult(true);

            var tcs = new TaskCompletionSource<bool>();
            process.EnableRaisingEvents = true;
            process.Exited += (sender, args) => tcs.TrySetResult(true);
            if (cancellationToken != default(CancellationToken))
                cancellationToken.Register(() => tcs.TrySetResult(false));

            return process.HasExited ? Task.FromResult(true) : tcs.Task;
        }
    }
}