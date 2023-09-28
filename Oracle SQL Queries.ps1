#
# Microsoft SQL.ps1 - IDM System PowerShell Script for Microsoft SQL Server.
#
# Any IDM System PowerShell Script is dot-sourced in a separate PowerShell context, after
# dot-sourcing the IDM Generic PowerShell Script '../Generic.ps1'.
#


$Log_MaskableKeys = @(
    'password'
)


#
# System functions
#

function Idm-SystemInfo {
    param (
        # Operations
        [switch] $Connection,
        [switch] $TestConnection,
        [switch] $Configuration,
        # Parameters
        [string] $ConnectionParams
    )

    Log info "-Connection=$Connection -TestConnection=$TestConnection -Configuration=$Configuration -ConnectionParams='$ConnectionParams'"
    
    if ($Connection) {
        @(
            @{
                name = 'DataProvider'
                type = 'combo'
                label = 'Data Provider'
                description = 'Data Provider for Oracle SQL server'
                table = @{
                    rows = @(
                        # Get ODP.NET_Managed_ODAC122cR1.zip from https://www.oracle.com/database/technologies/odac-downloads.html
                        # Install using elevated command prompt: install_odpm.bat D:\Oracle x64 true
                        @{ id = '0'; display_text = 'Oracle Data Provider for .NET - Managed Driver' }
                    )
                    settings_combo = @{
                        value_column = 'id'
                        display_column = 'display_text'
                    }
                }
                value = '0'
            }
            @{
                name = 'ODP_NET_InstallPath'
                type = 'textbox'
                label = 'ODP.NET installation path'
                description = 'Path of ODP.NET installation'
                value = ''
            }
            @{
                name = 'DataSource'
                type = 'textbox'
                label = 'Data Source'
                description = 'Data Source of Oracle SQL server'
                value = ''
            }
            @{
                name = 'UseSvcAccountCreds'
                type = 'checkbox'
                label = 'Use credentials of service account'
                value = $true
            }
            @{
                name = 'Username'
                type = 'textbox'
                label = 'Username'
                label_indent = $true
                description = 'User account name to access Oracle SQL server'
                value = ''
                hidden = 'UseSvcAccountCreds'
            }
            @{
                name = 'Password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                description = 'User account password to access Oracle SQL server'
                value = ''
                hidden = 'UseSvcAccountCreds'
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                description = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                description = ''
                value = 30
            }
            @{
                name = 'table_1_name'
                type = 'textbox'
                label = 'Query 1 - Name of Table'
                description = ''
            }
            @{
                name = 'table_1_query'
                type = 'textbox'
                label = 'Query 1 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_2_name'
                type = 'textbox'
                label = 'Query 2 - Name of Table'
                description = ''
            }
            @{
                name = 'table_2_query'
                type = 'textbox'
                label = 'Query 2 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_3_name'
                type = 'textbox'
                label = 'Query 3 - Name of Table'
                description = ''
            }
            @{
                name = 'table_3_query'
                type = 'textbox'
                label = 'Query 3 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_4_name'
                type = 'textbox'
                label = 'Query 4 - Name of Table'
                description = ''
            }
            @{
                name = 'table_4_query'
                type = 'textbox'
                label = 'Query 4 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_5_name'
                type = 'textbox'
                label = 'Query 5 - Name of Table'
                description = ''
            }
            @{
                name = 'table_5_query'
                type = 'textbox'
                label = 'Query 5 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_6_name'
                type = 'textbox'
                label = 'Query 6 - Name of Table'
                description = ''
            }
            @{
                name = 'table_6_query'
                type = 'textbox'
                label = 'Query 6 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_7_name'
                type = 'textbox'
                label = 'Query 7 - Name of Table'
                description = ''
            }
            @{
                name = 'table_7_query'
                type = 'textbox'
                label = 'Query 7 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_8_name'
                type = 'textbox'
                label = 'Query 8 - Name of Table'
                description = ''
            }
            @{
                name = 'table_8_query'
                type = 'textbox'
                label = 'Query 8 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_9_name'
                type = 'textbox'
                label = 'Query 9 - Name of Table'
                description = ''
            }
            @{
                name = 'table_9_query'
                type = 'textbox'
                label = 'Query 9 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_10_name'
                type = 'textbox'
                label = 'Query 10 - Name of Table'
                description = ''
            }
            @{
                name = 'table_10_query'
                type = 'textbox'
                label = 'Query 10 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_11_name'
                type = 'textbox'
                label = 'Query 11 - Name of Table'
                description = ''
            }
            @{
                name = 'table_11_query'
                type = 'textbox'
                label = 'Query 11 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_12_name'
                type = 'textbox'
                label = 'Query 12 - Name of Table'
                description = ''
            }
            @{
                name = 'table_12_query'
                type = 'textbox'
                label = 'Query 12 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_13_name'
                type = 'textbox'
                label = 'Query 13 - Name of Table'
                description = ''
            }
            @{
                name = 'table_13_query'
                type = 'textbox'
                label = 'Query 13 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_14_name'
                type = 'textbox'
                label = 'Query 14 - Name of Table'
                description = ''
            }
            @{
                name = 'table_14_query'
                type = 'textbox'
                label = 'Query 14 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_15_name'
                type = 'textbox'
                label = 'Query 15 - Name of Table'
                description = ''
            }
            @{
                name = 'table_15_query'
                type = 'textbox'
                label = 'Query 15 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_16_name'
                type = 'textbox'
                label = 'Query 16 - Name of Table'
                description = ''
            }
            @{
                name = 'table_16_query'
                type = 'textbox'
                label = 'Query 16 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_17_name'
                type = 'textbox'
                label = 'Query 17 - Name of Table'
                description = ''
            }
            @{
                name = 'table_17_query'
                type = 'textbox'
                label = 'Query 17 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_18_name'
                type = 'textbox'
                label = 'Query 18 - Name of Table'
                description = ''
            }
            @{
                name = 'table_18_query'
                type = 'textbox'
                label = 'Query 18 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_19_name'
                type = 'textbox'
                label = 'Query 19 - Name of Table'
                description = ''
            }
            @{
                name = 'table_19_query'
                type = 'textbox'
                label = 'Query 19 - SQL Statement'
                description = ''
            }
            @{
                name = 'table_20_name'
                type = 'textbox'
                label = 'Query 20 - Name of Table'
                description = ''
            }
            @{
                name = 'table_20_query'
                type = 'textbox'
                label = 'Query 20 - SQL Statement'
                description = ''
            }
        )
    }

    if ($TestConnection) {
        Open-OracleSqlConnection $ConnectionParams
    }

    if ($Configuration) {
        @()
    }

    Log info "Done"
}


function Idm-OnUnload {
    Close-OracleSqlConnection
}

#
# CRUD functions
#

$ColumnsInfoCache = @{}

$SqlInfoCache = @{}

function Fill-SqlInfoCache {
    param (
        [switch] $Force,
        [string] $Query,
        [string] $Class
    )

    # Refresh cache
    $sql_command = New-OracleSqlCommand $Query
    $result = (Invoke-OracleSqlCommand $sql_command) | Get-Member -MemberType Properties | Select-Object Name
    
    Dispose-OracleSqlCommand $sql_command

    $objects = New-Object System.Collections.ArrayList
    $object = @{}
    # Process in one pass
    foreach ($row in $result) {
            $object = @{
                full_name = $Class
                type      = 'Query'
                columns   = New-Object System.Collections.ArrayList
            }

        $object.columns.Add(@{
            name           = $row.Name
            is_primary_key = $false
            is_identity    = $false
            is_computed    = $false
            is_nullable    = $true
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }
    @($objects)
}


function Idm-Dispatcher {
    param (
        # Optional Class/Operation
        [string] $Class,
        [string] $Operation,
        # Mode
        [switch] $GetMeta,
        # Parameters
        [string] $SystemParams,
        [string] $FunctionParams
    )

    Log info "-Class='$Class' -Operation='$Operation' -GetMeta=$GetMeta -SystemParams='$SystemParams' -FunctionParams='$FunctionParams'"
    $connection_params = ConvertFrom-Json2 $SystemParams

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-OracleSqlConnection $SystemParams
            
            #
            # Output list of supported operations per table/view (named Class)
            #
            for ($i = 0; $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name".length -gt 0)
                {
                    @(
                        [ordered]@{
                            Class = $connection_params."table_$($i)_name"
                            Operation = 'Read'
                            'Source type' = 'Query'
                            'Primary key' = ''
                            'Supported operations' = 'R'
                            'Query' = $connection_params."table_$($i)_query"
                        }
                    )
                    }
            }
        }
        else {
            # Purposely no-operation.
        }

    }
    else {

        if ($GetMeta) {
            #
            # Get meta data
            #

            Open-OracleSqlConnection $SystemParams

            @()

        }
        else {
            #
            # Execute function
            #

            Open-OracleSqlConnection $SystemParams

            for ($i = 0; $i -lt 21; $i++)
            {
                if($connection_params."table_$($i)_name" -eq $class)
                {
                    $class_query = ($connection_params."table_$($i)_query" -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
                    break
                }
            }

            $column_query = "{0} OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY" -f $class_query
        
            $columns = Fill-SqlInfoCache -Query $column_query
        
            $Global:ColumnsInfoCache[$Class] = @{
                primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }
            
            $sql_command = New-OracleSqlCommand $class_query
            Invoke-OracleSqlCommand $sql_command
            Dispose-OracleSqlCommand $sql_command

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function New-OracleSqlCommand {
    param (
        [string] $CommandText
    )

    $sql_command = New-Object Oracle.ManagedDataAccess.Client.OracleCommand($CommandText, $Global:OracleSqlConnection)
    $sql_command.BindByName = $true

    return $sql_command
}


function Dispose-OracleSqlCommand {
    param (
        [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand
    )

    $SqlCommand.Dispose()
}

function Invoke-OracleSqlCommand {
    param (
        [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand,
        [string] $DeParamCommand
    )

    # Streaming
    function Invoke-OracleSqlCommand-ExecuteReader {
        param (
            [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand
        )
        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            $hash_table = [ordered]@{}

            foreach ($column_name in $column_names) {
                $hash_table[$column_name] = ""
            }

            $obj = New-Object -TypeName PSObject -Property $hash_table

            # Read data
			if($data_reader.HasRows) {
				while ($data_reader.Read()) {
					foreach ($column_name in $column_names) {
						$obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
					}

					# Output data
					$obj
				}
			} else { [PSCustomObject]$hash_table }
        }

        $data_reader.Close()
    }

    # Non-streaming (data stored in $data_set)
    try {
        Invoke-OracleSqlCommand-ExecuteReader $SqlCommand
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }

    Log debug "Done"
}


function Open-OracleSqlConnection {
    param (
        [string] $ConnectionParams
    )

    $connection_params = ConvertFrom-Json2 $ConnectionParams

    Add-Type -Path "$($connection_params.ODP_NET_InstallPath -replace '[/\\]?odp.net[/\\]?$')\odp.net\managed\common\Oracle.ManagedDataAccess.dll"

    $cs_builder = New-Object Oracle.ManagedDataAccess.Client.OracleConnectionStringBuilder

    # Use connection related parameters only
    $cs_builder['Data Source'] = $connection_params.DataSource

    if ($connection_params.UseSvcAccountCreds) {
        # None
    }
    else {
        $cs_builder['User ID']  = $connection_params.Username
        $cs_builder['Password'] = $connection_params.Password
    }

    $connection_string = $cs_builder.ConnectionString

    if ($Global:OracleSqlConnection -and $connection_string -ne $Global:OracleSqlConnectionString) {
        Log info "OracleSqlConnection connection parameters changed"
        Close-OracleSqlConnection
    }

    if ($Global:OracleSqlConnection -and $Global:OracleSqlConnection.State -ne 'Open') {
        Log warn "OracleSqlConnection State is '$($Global:OracleSqlConnection.State)'"
        Close-OracleSqlConnection
    }

    if ($Global:OracleSqlConnection) {
        #Log debug "Reusing OracleSqlConnection"
    }
    else {
        Log info "Opening OracleSqlConnection '$connection_string'"

        try {
            $connection = New-Object Oracle.ManagedDataAccess.Client.OracleConnection($connection_string)
            $connection.Open()

            $Global:OracleSqlConnection       = $connection
            $Global:OracleSqlConnectionString = $connection_string

            $Global:ColumnsInfoCache = @{}
            $Global:SqlInfoCache = @{}
        }
        catch {
            Log error "Failed: $_"
            Write-Error $_
        }

        Log info "Done"
    }
}


function Close-OracleSqlConnection {
    if ($Global:OracleSqlConnection) {
        Log info "Closing OracleSqlConnection"

        try {
            $Global:OracleSqlConnection.Close()
            $Global:OracleSqlConnection = $null
        }
        catch {
            # Purposely ignoring errors
        }

        Log info "Done"
    }
}
