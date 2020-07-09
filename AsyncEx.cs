using System;
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
    }
}