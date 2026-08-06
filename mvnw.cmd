@echo off
setlocal EnableExtensions EnableDelayedExpansion
set "BASE_DIR=%~dp0"
set "PROPERTIES=%BASE_DIR%.mvn\wrapper\maven-wrapper.properties"

for /f "usebackq tokens=1,* delims==" %%A in ("%PROPERTIES%") do (
  if "%%A"=="distributionUrl" set "DISTRIBUTION_URL=%%B"
  if "%%A"=="distributionSha256Sum" set "DISTRIBUTION_SHA256=%%B"
)

if not defined DISTRIBUTION_URL (
  echo Missing distributionUrl in %PROPERTIES% 1>&2
  exit /b 1
)

if not defined MAVEN_USER_HOME set "MAVEN_USER_HOME=%USERPROFILE%\.m2"
set "MAVEN_HOME=%MAVEN_USER_HOME%\wrapper\dists\apache-maven-3.9.11\mcs-scheduler\apache-maven-3.9.11"

if not exist "%MAVEN_HOME%\bin\mvn.cmd" (
  set "TMP_DIR=%TEMP%\mcs-maven-wrapper-%RANDOM%-%RANDOM%"
  mkdir "!TMP_DIR!" || exit /b 1
  powershell -NoProfile -ExecutionPolicy Bypass -Command ^
    "$ErrorActionPreference='Stop';" ^
    "$archive='!TMP_DIR!\apache-maven-3.9.11-bin.zip';" ^
    "Invoke-WebRequest -UseBasicParsing -Uri '%DISTRIBUTION_URL%' -OutFile $archive;" ^
    "if ('%DISTRIBUTION_SHA256%' -ne '') { if ((Get-FileHash $archive -Algorithm SHA256).Hash.ToLower() -ne '%DISTRIBUTION_SHA256%') { throw 'Maven distribution checksum mismatch' } };" ^
    "Expand-Archive -Path $archive -DestinationPath '!TMP_DIR!';" ^
    "New-Item -ItemType Directory -Force -Path (Split-Path '%MAVEN_HOME%') | Out-Null;" ^
    "Move-Item '!TMP_DIR!\apache-maven-3.9.11' '%MAVEN_HOME%'"
  if errorlevel 1 exit /b 1
  rmdir /s /q "!TMP_DIR!"
)

call "%MAVEN_HOME%\bin\mvn.cmd" %*
exit /b %ERRORLEVEL%
