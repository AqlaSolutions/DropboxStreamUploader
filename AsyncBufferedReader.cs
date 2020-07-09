using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace DropboxStreamUploader
{
    public class AsyncBufferedReader
    {
        readonly Stream _stream;
        readonly MemoryStream _buffer = new MemoryStream();

        public AsyncBufferedReader(Stream stream)
        {
            _stream = stream;
        }

        public async Task Start(int operationBufferSize, CancellationToken cancellation)
        {
            byte[] operationBuffer = new byte[operationBufferSize];
            while (!cancellation.IsCancellationRequested && _stream.CanRead)
            {
                int read = await _stream.ReadAsync(operationBuffer, 0, operationBufferSize, cancellation);
                if (read == 0)
                {
                    await Task.Delay(500, cancellation);
                    continue;
                }

                lock (_buffer)
                    _buffer.Write(operationBuffer, 0, read);
            }
        }

        public bool IsDataAvailable
        {
            get
            {
                lock (_buffer) return _buffer.Length > 0;
            }
        }

        public byte[] Buffer { get; private set; }

        public int Advance()
        {
            lock (_buffer)
            {
                _buffer.Position = 0;

                int r;
                if ((Buffer != null) && (Buffer.Length >= _buffer.Length))
                    r = _buffer.Read(Buffer, 0, Buffer.Length);
                else
                {
                    Buffer = _buffer.ToArray();
                    r = Buffer.Length;
                }

                _buffer.SetLength(0);


                return r;
            }
        }
    }
}