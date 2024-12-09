function loader {
    <#
    .SYNOPSIS
    Loads necessary PowerShell modules for branch and commit comparisons.

    .DESCRIPTION
    Imports the required modules `get-objectsBetweenBranches` and `get-objectsBetweenCommits` 
    to support operations in the deployment script.

    .EXAMPLE
    loader
    # Loads the required modules to work with the deployment script.
    #>
    Import-Module -Name ".\get-objectsBetweenBranches.ps1"
    Import-Module -Name ".\get-objectsBetweenCommits.ps1"
}

loader

function create-deploymentScript {
    <#
    .SYNOPSIS
    Generates deployment scripts based on differences in database objects between branches or commits.

    .DESCRIPTION
    This function creates deployment scripts by comparing SQL objects between specified branches 
    or commits. The generated scripts are categorized into tables, views, and stored procedures.

    .PARAMETER targetBranch
    The target branch for comparison. Required for branch-based comparisons.

    .PARAMETER schema
    Filter objects by schema. Use "ALL" to include all schemas.

    .PARAMETER objectType
    Filter objects by type (e.g., table, view, procedure). Use "ALL" to include all object types.

    .PARAMETER baseCommit
    The base commit hash for commit comparison.

    .PARAMETER targetCommit
    The target commit hash for commit comparison.

    .PARAMETER targetSolution
    The name of the solution being deployed.

    .PARAMETER useBranchDiff
    Set to $true for branch comparison; otherwise, commit comparison is used.

    .PARAMETER sourceBranch
    The source branch for branch comparison.

    .EXAMPLE
    create-deploymentScript -sourceBranch "release/1.2" -targetBranch "release/1.3" -useBranchDiff $true -schema "ALL" -objectType "ALL"
    # Generates deployment scripts for all objects between the two branches.

    .EXAMPLE
    create-deploymentScript -targetBranch "main" -baseCommit "abc123" -targetCommit "def456" -targetSolution "MySolution" -schema "dbo" -objectType "table"
    # Generates deployment scripts for all tables in the dbo schema between two commits.

    .NOTES
    Ensure that the required modules (`get-objectsBetweenBranches.ps1` and `get-objectsBetweenCommits.ps1`) 
    are loaded using the `loader` function.
    #>

    param (
        [string]$targetBranch = "*****",
        [string]$schema = "",
        [string]$objectType = "",
        [string]$baseCommit = "",
        [string]$targetCommit = "",
        [string]$targetSolution = "",
        [bool]$useBranchDiff = $false,
        [string]$sourceBranch = ""
    )

    # Validate parameters for branch comparison
    if ($useBranchDiff -and $targetBranch -eq "") {
        Write-Error "Please provide a target branch for branch comparison."
        exit
    }

    # Retrieve objects based on the comparison type
    $objects = if ($useBranchDiff) {
        get-objectsBetweenBranches -baseBranch $sourceBranch -targetBranch $targetBranch -TargetSolution $targetSolution | Where-Object { $_ -like '*.sql' }
    } else {
        get-objectsBetweenCommits -branchname $targetBranch -baseCommit $baseCommit -targetCommit $targetCommit -TargetSolution $targetSolution | Where-Object { $_ -like '*.sql' }
    }

    # Apply filtering based on schema and object type
    $objectsToScript = switch ($schema.ToUpper()) {
        "ALL" { if ($objectType.ToUpper() -eq "ALL") { $objects } else { $objects | Where-Object { $_ -like "*$objectType*" } } }
        default { if ($objectType.ToUpper() -eq "ALL") { $objects | Where-Object { $_ -like "*$schema*" } } else { $objects | Where-Object { $_ -like "*$schema*" -and $_ -like "*$objectType*" } } }
    }

    # Generate output file names
    $date = Get-Date -Format "yyyyMMddHHmm"
    $outputFiles = @{
        Table = "c:\temp\Output_${targetSolution}_${targetBranch.Replace('/', '-')}_${date}_table.sql"
        View  = "c:\temp\Output_${targetSolution}_${targetBranch.Replace('/', '-')}_${date}_view.sql"
        SP    = "c:\temp\Output_${targetSolution}_${targetBranch.Replace('/', '-')}_${date}_storedprocedure.sql"
    }

    # Remove existing output files
    foreach ($file in $outputFiles.Values) {
        if (Test-Path -Path $file) {
            Remove-Item $file
        }
    }

    # Process objects and generate scripts
    foreach ($script in $objectsToScript) {
        $linenumber = Select-String -Path $script -Pattern "(?<=CREATE|ALTER)[ ](.+)[ ](?=\[\w+\]\.)" -List | Select-Object -ExpandProperty LineNumber
        if (-not $linenumber) { continue }

        $objectName = Select-String -Path $script -Pattern "\[\w+\]\.\[\w+\]" | Where { $_.LineNumber -eq $linenumber } | ForEach-Object { $_.Matches.Groups[0].Value }
        $objectType = Select-String -Path $script -Pattern "(?<=CREATE|ALTER)[ ](.+)[ ](?=\[\w+\]\.)" | Where { $_.LineNumber -eq $linenumber } | ForEach-Object { $_.Matches.Groups[0].Value }
        $dropStatement = "`r`nIF OBJECT_ID('$objectName') IS NOT NULL`r`nDROP $objectType $objectName`r`nGO`r`n"

        Write-Host "Scripting object $objectName from script $script" -ForegroundColor DarkGreen

        switch ($script) {
            "*table*" { Add-Content $outputFiles.Table $dropStatement; Add-Content $outputFiles.Table (Get-Content $script | Select-Object -Skip ($linenumber - 1)); Add-Content $outputFiles.Table "`r`nGO" }
            "*view*"  { Add-Content $outputFiles.View $dropStatement; Add-Content $outputFiles.View (Get-Content $script | Select-Object -Skip ($linenumber - 1)); Add-Content $outputFiles.View "`r`nGO" }
            "*procedure*" { Add-Content $outputFiles.SP $dropStatement; Add-Content $outputFiles.SP (Get-Content $script | Select-Object -Skip ($linenumber - 1)); Add-Content $outputFiles.SP "`r`nGO" }
        }
    }
}
