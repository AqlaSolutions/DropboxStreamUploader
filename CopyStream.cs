using System;
using System.IO;

namespace DropboxStreamUploader
{
    public class CopyStream : Stream
    {
        public CopyStream(Stream copyTo)
        {
            CopyTo = copyTo;
        }

        public CopyStream()
        {
        }

        public Stream CopyTo { get; set; }
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => _position;

        public override long Seek(long offset, SeekOrigin loc)
        {
            throw new NotSupportedException();
        }

        public override void SetLength(long value)
        {
            throw new NotSupportedException();
        }

        public override void Flush()
        {
            CopyTo.Flush();
        }

        public override int Read(byte[] buffer, int offset, int count)
        {
            throw new NotSupportedException();
        }

        public override void Write(byte[] buffer, int offset, int count)
        {
            CopyTo.Write(buffer, 0, count);
            _position += count;
        }

        long _position;
        public override long Position { get => _position; set => throw new NotSupportedException(); }
        
    }
}