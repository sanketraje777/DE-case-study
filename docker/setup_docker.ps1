# Run as Administrator

# What this script does:
#   Installs Chocolatey if missing
#   Enables WSL and Virtual Machine Platform features
#   Downloads and installs the WSL2 kernel update
#   Installs Ubuntu 22.04 as the default WSL distro
#   Installs Docker Desktop
#   Informs you to restart to finalize everything

Write-Host "Updating package sources..."

# Check if Chocolatey is installed
if (!(Get-Command choco -ErrorAction SilentlyContinue)) {
    Write-Host "Installing Chocolatey..."
    Set-ExecutionPolicy Bypass -Scope Process -Force
    [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.SecurityProtocolType]::Tls12
    Invoke-Expression ((New-Object System.Net.WebClient).DownloadString('https://community.chocolatey.org/install.ps1'))
} else {
    Write-Host "Chocolatey is already installed."
}

Write-Host "Updating Chocolatey..."
choco upgrade chocolatey -y

# Enable WSL feature
Write-Host "Enabling WSL..."
dism.exe /online /enable-feature /featurename:Microsoft-Windows-Subsystem-Linux /all /norestart

# Enable Virtual Machine Platform
Write-Host "Enabling Virtual Machine Platform..."
dism.exe /online /enable-feature /featurename:VirtualMachinePlatform /all /norestart

# Set WSL version to 2
Write-Host "Setting WSL2 as default..."
wsl --set-default-version 2

# Download and install the latest WSL2 kernel update
Write-Host "Downloading WSL2 Kernel Update..."
$wslUpdateUrl = "https://wslstorestorage.blob.core.windows.net/wslblob/wsl_update_x64.msi"
$wslUpdateInstaller = "$env:TEMP\wsl_update_x64.msi"
Invoke-WebRequest -Uri $wslUpdateUrl -OutFile $wslUpdateInstaller
Start-Process msiexec.exe -ArgumentList "/i $wslUpdateInstaller /quiet /norestart" -Wait
Remove-Item $wslUpdateInstaller

Write-Host "WSL2 Kernel Update installed."

# Install Ubuntu as the default WSL distro
Write-Host "Installing Ubuntu..."
choco install wsl-ubuntu-2204 -y

# Install Docker Desktop
Write-Host "Installing Docker Desktop..."
choco install docker-desktop -y

Write-Host "Docker Desktop installed!"

Write-Host "`nPlease restart your computer to finalize the WSL2 and Docker Desktop setup."
Write-Host "After restart, launch Docker Desktop and ensure it is using the WSL2 backend (recommended)."
Write-Host "You can also run Ubuntu by typing 'wsl' or 'ubuntu' in the Start Menu."
