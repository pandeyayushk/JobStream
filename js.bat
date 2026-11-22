@echo off
setlocal
set "JAR=%~dp0target\jobstream-1.0-SNAPSHOT.jar"
if not exist "%JAR%" (
  echo JobStream JAR not found: "%JAR%"
  echo Build it first with: mvn package
  endlocal & exit /b 1
)
java -jar "%JAR%" %*
set "EXIT_CODE=%ERRORLEVEL%"
endlocal & exit /b %EXIT_CODE%
