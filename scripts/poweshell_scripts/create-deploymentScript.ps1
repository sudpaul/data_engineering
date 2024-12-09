function loader {
    Import-Module -Name ".\get-objectsBetweenBranches.ps1"
    Import-Module -Name ".\get-objectsBetweenCommits.ps1"
}

loader
function create-deploymentScript 
( $targetbranch ="*****",
  $schema = "",
  $objectType = "",
  $baseCommit = "",
  $targetCommit = "",
  $targetSolution = "",
  $useBranchDiff = $false,
  $sourceBranch = ""

) {
    if($useBranchDiff -and $targetBranch -eq ""){
        Write-Error "Please provide Target branch in case of Branch comparsion"
        exit        
    }

    if ($useBranchDiff) {
        $objects = get-objectsBetweenBranches -baseBranch $sourceBranch -targetBranch $targetBranch -TargetSolution $targetSolution  | Where-Object {$_ -Like '*.sql'}
    }
    else {
        $objects = get-objectsBetweenCommits -branchname $targetbranch -baseCommit $baseCommit -targetCommit $targetCommit -TargetSolution $targetSolution |  Where-Object {$_ -Like '*.sql'}
    }
    
    if($schema.ToUpper() -eq "ALL" -AND $objectType.ToUpper() -ne "ALL"){
    
        $objectsToScript = $objects | Where-Object {$_ -like "*" + $objectType + "*"}
    }
    elseif($schema.ToUpper() -ne "ALL" -AND $objectType.ToUpper() -eq "ALL"){
        
        $objectsToScript = $objects | Where-Object {$_ -like "*" + $schema + "*"}
    }
    elseif($schema.ToUpper() -eq "ALL" -AND $objectType.ToUpper() -eq "ALL"){
        $objectsToScript = $objects
    }
    else {
        $objectsToScript = $objects | Where-Object {$_ -like "*" + $schema + "*" -and $_ -like "*" + $objectType + "*"}
    }
    
    
    $date = Get-Date -Format "yyyymmddHHmm"
    $outpuTableFile = "c:\temp\Output_" + $targetSolution + "_" + $targetbranch.replace("/","-") + "_" + $date + "_table.sql"
    $outpuViewFile = "c:\temp\Output_" + $targetSolution + "_" + $targetbranch.replace("/","-") + "_" + $date + "_view.sql"
    $outpuSPFile = "c:\temp\Output_" + $targetSolution + "_" + $targetbranch.replace("/","-") + "_" + $date + "_storedprocedure.sql"


    if(test-path -Path $outpuTableFile) {
        Remove-Item $outpuTableFile
    }

    if(test-path -Path $outpuViewFile) {
        Remove-Item $outpuViewFile
    }

    if(test-path -Path $outpuSPFile) {
        Remove-Item $outpuSPFile
    }
    
    $linenumber = 0
    
    foreach($script in $objectsToScript) {
        $isAlter = $false
        #(Get-Content $script).replace("Alter","Create") | Set-Content $script
        #(Get-Content $script).replace(";","`r`nGo") | Set-Content $script
        $linenumber = Select-String -path $script -Pattern "(?<=CREATE)[ ](.+)[ ](?=\[\w+\]\.)" -list | Select-Object -ExpandProperty Linenumber
        
        # Created statement not found, search for alter
        if(($linenumber -eq 0) -or ($linenumber -eq $null)){
            $linenumber = Select-String -path $script -Pattern "(?<=ALTER)[ ](.+)[ ](?=\[\w+\]\.)" -list | Select-Object -ExpandProperty Linenumber
            $isAlter = $true
        }

        $objectName = Select-String -path $script -Pattern "\[\w+\]\.\[\w+\]" | where {$_.LineNumber -eq $linenumber} |  foreach {$_.Matches.Groups[0].Value}
        $objectType = Select-String -path $script -Pattern "(?<=CREATE)[ ](.+)[ ](?=\[\w+\]\.)" | where {$_.LineNumber -eq $linenumber} | foreach {$_.Matches.Groups[0].Value}
        $dropStatement = "`r`nIF OBJECT_ID('$objectName') IS NOT NULL"
        $dropStatement += "`r`nDrop " + $objectType + " " + $objectName
        $dropStatement += "`r`n Go `r`n"

        write-host "Scripting Object $objectName from script $script" -ForegroundColor DarkGreen

        if($script -like "*table*"){

            if(!$isAlter){
                Add-Content $outpuTableFile $dropStatement 
            } 
            
            $tableSql = Get-Content -Path $script | Select-Object -Skip ($linenumber - 1) 
            
            Add-Content $outpuTableFile $tableSql 

            Add-Content $outpuTableFile "`r`n Go" 
            
        }

        if($script -like "*view*"){

            if(!$isAlter){
                Add-Content $outpuViewFile $dropStatement            
            }
            
            $viewSql = Get-Content -Path $script | Select-Object -Skip ($linenumber - 1) 
            
            Add-Content $outpuViewFile $viewSql

            Add-Content $outpuViewFile "`r`n Go"
            
        }

        if($script -like "*procedure*"){

            if(!$isAlter){
                Add-Content $outpuSPFile $dropStatement
            }

            $storedProcedureSql = Get-Content -Path $script | Select-Object -Skip ($linenumber - 1) 
            
            Add-Content $outpuSPFile $storedProcedureSql

            Add-Content $outpuSPFile "`r`n Go"
        }
               
                
        #$objectName = Select-String -path $file.FullName -Pattern "\[$schemaName\].*\]" -List | foreach {$_.Matches.Groups[0].Value}
        #$existString = "`r`nIF OBJECT_ID('$objectName') IS NOT NULL"
        #$dropContent = Get-Content $file.FullName
    }
     
}

#create-deploymentScript -sourceBranch "release/Release1.2" -targetBranch "release/Release1.3" -useBranchDiff $true -objectType "All" -schema "All"
