# Function to find the "GO" command in a SQL script file
function Find-GoInSQLScript {
    param (
        [Parameter(Mandatory = $true, HelpMessage = "Path to the SQL script file")]
        [string]$FilePath
    )

    # Initialize variables
    $success = $false
    $totalLines = (Get-Content -Path $FilePath -ErrorAction Stop | Measure-Object -Line).Lines

    # Loop through each line in the file
    for ($count = 1; $count -le $totalLines; $count++) {
        $lines = Get-Content -Path $FilePath -Tail $count

        # Check if "GO" statement exists
        if ($lines -match "^\s*GO\s*$") {
            $success = $true
            Write-Output "Found 'GO' statement at line $count."
            break
        }

        # Check for any other non-empty statement
        $exit = $lines | ForEach-Object { $_.Trim() -ne "" }
        if ($exit -contains $true) {
            Write-Output "Non-empty statement detected before finding 'GO'. Exiting search."
            break
        }
    }

    # Return the result
    return $success
}

# Example Usage
# Specify the path to your SQL script file
$FilePath = "C:\temp\TestSQL\Output_Datawarehouse_1.0.0.56_storedprocedure.sql"

# Call the function
$result = Find-GoInSQLScript -FilePath $FilePath

# Output the result
if ($result) {
    Write-Host "The 'GO' command was found in the SQL script." -ForegroundColor Green
} else {
    Write-Host "No 'GO' command found in the SQL script." -ForegroundColor Red
}
