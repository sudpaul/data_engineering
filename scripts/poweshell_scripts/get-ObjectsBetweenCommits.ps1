<#
.SYNOPSIS
    Retrieves a list of objects modified between two Git commits in a specified branch and repository.

.DESCRIPTION
    This function clones or resets a local Git repository, checks out a branch, and identifies the files
    changed between two specified commits. It supports filtering the results to match a specific solution
    or retrieving all modified files.

.PARAMETER localWorkspacePath
    The local directory path for the Git repository clone or existing workspace. Default is 'C:\Temp\VSTS1'.

.PARAMETER branchName
    The Git branch to work on. Default is 'master'.

.PARAMETER baseCommit
    The commit hash for the starting point of the diff.

.PARAMETER targetCommit
    The commit hash for the ending point of the diff. Default is 'HEAD'.

.PARAMETER targetSolution
    A specific solution or folder to filter the modified objects. Default is 'Datawarehouse'.

.PARAMETER forceClone
    If set to $true, forces a fresh clone of the repository by deleting any existing workspace. Default is $false.

.EXAMPLE
    $obj = get-objectsBetweenCommits -branchName "feature/MyFeature" -baseCommit "abc123" -targetCommit "def456"

.NOTES
    Ensure Git is installed and configured on the system, and the appropriate permissions exist for the repository.
#>

function Get-ObjectsBetweenCommits {
    [CmdletBinding()]
    param (
        [string]$localWorkspacePath = '****',
        [string]$branchName = '****',
        [string]$baseCommit,
        [string]$targetCommit = "****",
        [string]$targetSolution = '****',
        [switch]$forceClone
    )

    # Force clone the repository if specified
    if ($forceClone) {
        Write-Verbose "Forcing repository clone by cleaning workspace at $localWorkspacePath"
        Get-ChildItem -Path $localWorkspacePath -Recurse -Force | Remove-Item -Force -Recurse -ErrorAction SilentlyContinue
        Remove-Item -Path $localWorkspacePath -Recurse -Force -ErrorAction SilentlyContinue
    }

    # Clone repository if the workspace doesn't exist
    if (!(Test-Path -Path $localWorkspacePath)) {
        Write-Verbose "Cloning repository into $localWorkspacePath"
        git clone -q "#clone_location" $localWorkspacePath
    }

    # Navigate to the local workspace
    Set-Location -Path $localWorkspacePath

    # Reset, checkout, fetch, and pull latest updates
    Write-Verbose "Resetting and syncing branch $branchName"
    git reset --hard | Out-Null
    git checkout -q $branchName | Out-Null
    git fetch -q | Out-Null
    git pull -q origin $branchName | Out-Null

    # Retrieve the list of changed files between commits
    Write-Verbose "Retrieving objects changed between $baseCommit and $targetCommit"
    if (($targetSolution.ToUpper() -eq "ALL") -or ($targetSolution -eq "") -or ($targetSolution -eq $null)) {
        $diffObjects = git diff --name-only --diff-filter=d $baseCommit $targetCommit
    } else {
        $diffObjects = git diff --name-only --diff-filter=d $baseCommit $targetCommit | Where-Object { $_ -like "$targetSolution*" }
    }

    # Save the list of changes to a file
    $outputFile = "C:\Temp\commit_list.txt"
    $diffObjects | Set-Content -Path $outputFile
    Write-Verbose "List of changed objects saved to $outputFile"

    # Generate full paths for the changed objects
    $objects = $diffObjects | ForEach-Object { Join-Path -Path $localWorkspacePath -ChildPath $_ }

    # Return to the initial directory
    Set-Location -Path "C:\temp"

    return $objects
}

# Example usage
$obj = Get-ObjectsBetweenCommits -branchName "#base_branch" -baseCommit "#commit_id0" -targetCommit "target_commit"
$obj
