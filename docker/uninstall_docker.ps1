# Requires elevated (Admin) PowerShell session
Write-Output "Uninstalling Docker Desktop..."

# Uninstall Docker Desktop via Windows Installer
$docker = Get-WmiObject -Class Win32_Product | Where-Object { $_.Name -like "Docker*" }

if ($docker) {
    $docker.Uninstall()
    Write-Output "Docker Desktop uninstalled successfully!"
} else {
    Write-Output "Docker Desktop not found."
}

# Remove any lingering Docker Compose binary
if (Test-Path "C:\Program Files\Docker\Docker\resources\bin\docker-compose.exe") {
    Remove-Item "C:\Program Files\Docker\Docker\resources\bin\docker-compose.exe" -Force
}

Write-Output "If needed, manually delete any Docker data in 'C:\ProgramData\DockerDesktop' or user config files."

Write-Output "Docker cleanup complete!"
