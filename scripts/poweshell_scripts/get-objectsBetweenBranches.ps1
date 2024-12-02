# SYNOPSIS
# Retrieves a list of objects (files) that have been modified between two Git branches.

$1
function Get-ObjectsBetweenBranches {
    param (
        [Parameter(Mandatory = $false, HelpMessage = "Local workspace path for Git operations")]
        [string]$LocalWorkspacePath = 'C:\Temp\VSTS',

        [Parameter(Mandatory = $false, HelpMessage = "Output path for the diff file")]
        [string]$OutputPath = "C:\Temp",

        [Parameter(Mandatory = $false, HelpMessage = "Base branch for comparison")]
        [string]$BaseBranch = 'master',

        [Parameter(Mandatory = $false, HelpMessage = "Target branch for comparison")]
        [string]$TargetBranch = 'develop',

        [Parameter(Mandatory = $false, HelpMessage = "Target solution filter")]
        [string]$TargetSolution = "",

        [Parameter(Mandatory = $false, HelpMessage = "Use a new Git workspace")]
        [bool]$UseNewWorkspace = $false,

        [Parameter(Mandatory = $false, HelpMessage = "Force Git repository clone")]
        [bool]$ForceClone = $false,

        [Parameter(Mandatory = $false, HelpMessage = "Repository URL for cloning")]
        [string]$RepositoryURL = "https://example.visualstudio.com/DefaultCollection/Project/_git/Repository"
    )

    # Log the start of the function
    Write-Host "Running Get-ObjectsBetweenBranches..." -ForegroundColor Cyan

    # Handle forced cloning and workspace creation
    if ($ForceClone -and $UseNewWorkspace) {
        Write-Host "Deleting existing workspace..." -ForegroundColor Yellow
        Remove-Item -Path $LocalWorkspacePath -Recurse -Force -ErrorAction SilentlyContinue
    }

    if (!(Test-Path -Path $LocalWorkspacePath) -and $UseNewWorkspace) {
        Write-Host "Cloning new workspace..." -ForegroundColor Yellow
        git clone $RepositoryURL $LocalWorkspacePath
    }

    Set-Location -Path $LocalWorkspacePath

    # Prepare output file
    $OutFileName = "diff_branch_${BaseBranch}_${TargetBranch}.txt".Replace("/", "-")
    $OutputFilePath = Join-Path -Path $OutputPath -ChildPath $OutFileName

    if (!(Test-Path -Path $OutputPath)) {
        New-Item -Path $OutputPath -ItemType Directory -Force
    }

    # Perform Git operations if using a new workspace
    if ($UseNewWorkspace) {
        Write-Host "Running Git commands in new workspace..." -ForegroundColor Yellow
        git reset --hard | Out-Null
        git checkout $TargetBranch | Out-Null
        git pull origin $TargetBranch | Out-Null
    }

    $BaseBranchFull = "origin/$BaseBranch"
    $TargetBranchFull = "origin/$TargetBranch"

    # Get the list of changed objects
    Write-Host "Collecting list of changed objects..." -ForegroundColor Yellow
    if ([string]::IsNullOrEmpty($TargetSolution) -or $TargetSolution.ToUpper() -eq "ALL") {
        $DiffObjects = git log $TargetBranchFull --not $BaseBranchFull --name-only --pretty=format:"" | Select-Object -Unique
    } else {
        $DiffObjects = git log $TargetBranchFull --not $BaseBranchFull --name-only --pretty=format:"" |
            Where-Object { $_ -like "$TargetSolution*" } | Select-Object -Unique
    }

    # Write objects to output file
    $DiffObjects | Out-File -FilePath $OutputFilePath -Encoding UTF8

    # Return the list of object paths
    $Objects = $DiffObjects | ForEach-Object { Join-Path -Path $LocalWorkspacePath -ChildPath $_ }

    if ($UseNewWorkspace) {
        Set-Location -Path "C:\"
    }

    Write-Host "Finished Get-ObjectsBetweenBranches successfully." -ForegroundColor Green
    return $Objects
}

# SYNOPSIS
# Retrieves a list of objects (files) that have been modified between two Git branches.

# USAGE EXAMPLE
# This is an example of how anyone would type the command in PowerShell to run the script:
# Get-ObjectsBetweenBranches -BaseBranch "main" -TargetBranch "feature/new-feature" -UseNewWorkspace $true -ForceClone $false
