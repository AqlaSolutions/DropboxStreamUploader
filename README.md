# DropboxStreamUploader
Uploads RTSP video stream from IP camera to Dropbox every 2 MB [min 5, max 30 seconds].
The stream is splitted into 1-minutes video files. Each video file is encrypted with zip.
To reduce space usage it automatically deletes zips older than 1 hour but they still can be recovered within 30 days with Dropbox Pro account.
When upload fails or power shortage happens unencrypted video file is kept locally so can be viewed and deleted later.
Uses ffmpeg.
Currently tested with Dahua camera.
Doesn't do any re-encoding so no cpu load.

## Usage
DropboxStreamUploader.exe dropbox-app-token stream-url dropbox-folder-name encryption-password mpeg-executable-path offline-recording-directory-path
### Example
DropboxStreamUploader.exe "asdlakdfkfrefggfdgdfg-rgedfgd-adfsfdf3e" "rtsp://admin:password@192.168.1.2:554/cam/realmonitor?channel=1&subtype=0" "/Camera" "password" "c:\ffmpeg\bin\ffmpeg.exe" "d:\camera"

## How to get Dropbox token
http://99rabbits.com/get-dropbox-access-token/

## Where to get ffmpeg
https://ffmpeg.org/download.html
