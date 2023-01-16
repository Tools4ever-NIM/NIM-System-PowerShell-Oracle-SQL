#
# Oracle SQL.ps1 - IDM System PowerShell Script for Oracle SQL Server.
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
    $t_now = Get-Date
    
    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]600000) {
        return
    }
    
    # Refresh cache
    $Global:SqlInfoCache.Data = Invoke-OracleSqlCommand "
        SELECT
            AT.OWNER || '.' || AT.TABLE_NAME AS full_object_name,
            (CASE WHEN AO.OBJECT_TYPE = 'TABLE' THEN 'Table' WHEN AO.OBJECT_TYPE = 'VIEW' THEN 'View' ELSE 'Unknown' END) AS object_type,
            ATC.COLUMN_NAME,
            (CASE WHEN PKS.TABLE_NAME IS NULL THEN 0 ELSE 1 END) AS is_primary_key,
            (CASE WHEN ATC.IDENTITY_COLUMN = 'NO' THEN 0 ELSE 1 END) AS is_identity,
            (CASE WHEN ATC.VIRTUAL_COLUMN  = 'NO' THEN 0 ELSE 1 END) AS is_computed,
            (CASE WHEN ATC.NULLABLE        = 'N'  THEN 0 ELSE 1 END) AS is_nullable
        FROM
            SYS.ALL_TABLES AT
            INNER JOIN SYS.ALL_OBJECTS AO ON AT.OWNER = AO.OWNER AND AT.TABLE_NAME = AO.OBJECT_NAME
            INNER JOIN SYS.ALL_TAB_COLS ATC ON AT.OWNER = ATC.OWNER AND AT.TABLE_NAME = ATC.TABLE_NAME
            LEFT JOIN SYS.ALL_CONS_COLUMNS ACC ON AT.OWNER = ACC.OWNER AND AT.TABLE_NAME = ACC.TABLE_NAME AND ATC.COLUMN_NAME = ACC.COLUMN_NAME
            LEFT JOIN (
                SELECT
                    OWNER,
                    TABLE_NAME,
                    CONSTRAINT_NAME
                FROM
                    SYS.ALL_CONSTRAINTS
                WHERE
                    CONSTRAINT_TYPE = 'P'
            ) PKS ON AT.OWNER = PKS.OWNER AND AT.TABLE_NAME = PKS.TABLE_NAME AND ACC.CONSTRAINT_NAME = PKS.CONSTRAINT_NAME
        WHERE
            AT.OWNER NOT IN ('APPQOSSYS', 'CTXSYS', 'DBSFWUSER', 'DBSNMP', 'DVSYS', 'GSMADMIN_INTERNAL', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RQSYS', 'SYSTEM', 'WMSYS', 'XDB','SYS','LBACSYS') AND
            AT.TABLE_NAME NOT LIKE '%$%'
        UNION
        SELECT
            AT.OWNER || '.' || AT.VIEW_NAME AS full_object_name,
            (CASE WHEN AO.OBJECT_TYPE = 'TABLE' THEN 'Table' WHEN AO.OBJECT_TYPE = 'VIEW' THEN 'View' ELSE 'Unknown' END) AS object_type,
            ATC.COLUMN_NAME,
            (CASE WHEN PKS.TABLE_NAME IS NULL THEN 0 ELSE 1 END) AS is_primary_key,
            (CASE WHEN ATC.IDENTITY_COLUMN = 'NO' THEN 0 ELSE 1 END) AS is_identity,
            (CASE WHEN ATC.VIRTUAL_COLUMN  = 'NO' THEN 0 ELSE 1 END) AS is_computed,
            (CASE WHEN ATC.NULLABLE        = 'N'  THEN 0 ELSE 1 END) AS is_nullable
        FROM
            SYS.ALL_VIEWS AT
            INNER JOIN SYS.ALL_OBJECTS AO ON AT.OWNER = AO.OWNER AND AT.VIEW_NAME = AO.OBJECT_NAME
            INNER JOIN SYS.ALL_TAB_COLS ATC ON AT.OWNER = ATC.OWNER AND AT.VIEW_NAME = ATC.TABLE_NAME
            LEFT JOIN SYS.ALL_CONS_COLUMNS ACC ON AT.OWNER = ACC.OWNER AND AT.VIEW_NAME = ACC.TABLE_NAME AND ATC.COLUMN_NAME = ACC.COLUMN_NAME
            LEFT JOIN (
                SELECT
                    OWNER,
                    TABLE_NAME,
                    CONSTRAINT_NAME
                FROM
                    SYS.ALL_CONSTRAINTS
                WHERE
                    CONSTRAINT_TYPE = 'P'
            ) PKS ON AT.OWNER = PKS.OWNER AND AT.VIEW_NAME = PKS.TABLE_NAME AND ACC.CONSTRAINT_NAME = PKS.CONSTRAINT_NAME
        WHERE
            AT.OWNER NOT IN ('APPQOSSYS', 'CTXSYS', 'DBSFWUSER', 'DBSNMP', 'DVSYS', 'GSMADMIN_INTERNAL', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RQSYS', 'SYSTEM', 'WMSYS', 'XDB','SYS','LBACSYS') AND
            AT.VIEW_NAME NOT LIKE '%$%'
    "

    $Global:SqlInfoCache.Ts = $t_now
}


function Get-SqlTablesInfo {
    Fill-SqlInfoCache

    @( $Global:SqlInfoCache.Data | Select-Object -Unique -Property @{ name = 'name'; expression = {$_.full_object_name} }, @{ name = 'type'; expression = {$_.object_type} } )
}


function Get-SqlColumnsInfo {
    param (
        [String] $Table
    )

    Fill-SqlInfoCache

    @( $Global:SqlInfoCache.Data | Where-Object { $_.full_object_name -eq $Table } | Select-Object -Property '*' -ExcludeProperty @('full_object_name', 'object_type') )
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
    Log debug "test"
    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-OracleSqlConnection $SystemParams

            $tables = Get-SqlTablesInfo

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($t in $tables) {
                    $primary_key = @( Get-SqlColumnsInfo $t.name | Where-Object { $_.is_primary_key } | ForEach-Object { $_.column_name })[0]

                    if ($t.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $t.name
                            Operation = 'Read'
                            'Source type' = $t.type
                            'Primary key' = $primary_key
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $t.name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $t.name
                            Operation = 'Read'
                            'Source type' = $t.type
                            'Primary key' = $primary_key
                            'Supported operations' = "CR$(if ($primary_key) { 'UD' } else { '' })"
                        }

                        if ($primary_key) {
                            # Only supported if primary key is present
                            [ordered]@{
                                Class = $t.name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $t.name
                                Operation = 'Delete'
                            }
                        }
                    }
                }
            )

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

            $columns = Get-SqlColumnsInfo $Class

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.column_name;
                                    allowance = if ($_.is_identity -or $_.is_computed) { 'prohibited' } elseif (! $_.is_nullable) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Read' {
                    @(
                        @{
                            name = 'where_clause'
                            type = 'textbox'
                            label = 'Filter (SQL where-clause)'
                            description = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            description = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.column_name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Auto identity' }
                                            if ($_.is_computed)    { 'Computed' }
                                            if ($_.is_nullable)    { 'Nullable' }
                                        ) -join ' | '
                                    }
                                })
                                settings_grid = @{
                                    selection = 'multiple'
                                    key_column = 'name'
                                    checkbox = $true
                                    filter = $true
                                    columns = @(
                                        @{
                                            name = 'name'
                                            display_name = 'Name'
                                        }
                                        @{
                                            name = 'config'
                                            display_name = 'Configuration'
                                        }
                                    )
                                }
                            }
                            value = @($columns | ForEach-Object { $_.column_name })
                        }
                    )
                    break
                }

                'Update' {
                    @{
                        semantics = 'update'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.column_name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
                                }
                            }
                        )
                    }
                    break
                }

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.column_name
                                        allowance = 'mandatory'
                                    }
                                }
                            }
                            @{
                                name = '*'
                                allowance = 'prohibited'
                            }
                        )
                    }
                    break
                }
            }

        }
        else {
            #
            # Execute function
            #

            Open-OracleSqlConnection $SystemParams

            if (! $Global:ColumnsInfoCache[$Class]) {
                $columns = Get-SqlColumnsInfo $Class

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_key  = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.column_name })[0]
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.column_name })[0]
                }
            }

            $primary_key  = $Global:ColumnsInfoCache[$Class].primary_key
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            $command = $null

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { """$_""" }) -join ', ' }

            switch ($Operation) {
                'Create' {
                    $selection = if ($identity_col) {
                                     """$identity_col"" = SCOPE_IDENTITY()"
                                 }
                                 elseif ($primary_key) {
                                     """$primary_key"" = '$($function_params[$primary_key])'"
                                 }
                                 else {
                                     @($function_params.Keys | ForEach-Object { """$_"" = '$($function_params[$_])'" }) -join ' AND '
                                 }

                    $command = "INSERT INTO $Class (" + @($function_params.Keys | ForEach-Object { """$_""" }) -join ', ' + ") VALUES ($(@($function_params.Keys | ForEach-Object { "$(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" }) -join ', ')); SELECT $projection FROM $Class WHERE $selection"
                    break
                }

                'Read' {
                    $selection = if ($function_params['where_clause'].length -eq 0) { '' } else { ' WHERE ' + $function_params['where_clause'] }

                    $command = "SELECT $projection FROM $Class$selection"
                    break
                }

                'Update' {
                    $command = "UPDATE $Class SET " + @($function_params.Keys | ForEach-Object { if ($_ -ne $primary_key) { """$_"" = $(if ($function_params[$_] -ne $null) { "'$($function_params[$_])'" } else { 'null' })" } }) -join ', ' + " WHERE ""$primary_key"" = '$($function_params[$primary_key])'"
                    break
                }

                'Delete' {
                    $command = "DELETE $Class WHERE ""$primary_key"" = '$($function_params[$primary_key])'"
                    break
                }
            }

            if ($command) {
                LogIO info ($command -split ' ')[0] -In -Command $command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-OracleSqlCommand $command
                }
                else {
                    # Log output
                    $rv = Invoke-OracleSqlCommand $command
                    LogIO info ($command -split ' ')[0] -Out $rv

                    $rv
                }
            }

        }

    }

    Log info "Done"
}


#
# Helper functions
#

function Invoke-OracleSqlCommand {
    param (
        [string] $Command
    )

    # Streaming
    function Invoke-OracleSqlCommand-ExecuteReader {
        param (
            [string] $Command
        )

        $SQLCommand = New-Object Oracle.ManagedDataAccess.Client.OracleCommand($Command, $Global:OracleSqlConnection)
        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            $hash_table = [ordered]@{}

            foreach ($column_name in $column_names) {
                $hash_table[$column_name] = ""
            }

            $obj = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                $obj
            }

        }

        $data_reader.Close()
        $SQLCommand.Dispose()
    }

    $Command = ($Command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '

    Log debug $Command

    try {
        Invoke-OracleSqlCommand-ExecuteReader $Command
    }
    catch {
        Log error "Failed: $_"
        Write-Error $_
    }
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
