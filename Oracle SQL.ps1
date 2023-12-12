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
                tooltip = 'Data Provider for Oracle SQL server'
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
                tooltip = 'Path of ODP.NET installation'
                value = ''
            }
            @{
                name = 'DataSource'
                type = 'textbox'
                label = 'Data Source'
                tooltip = 'Data Source of Oracle SQL server'
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
                tooltip = 'User account name to access Oracle SQL server'
                value = ''
                hidden = 'UseSvcAccountCreds'
            }
            @{
                name = 'Password'
                type = 'textbox'
                password = $true
                label = 'Password'
                label_indent = $true
                tooltip = 'User account password to access Oracle SQL server'
                value = ''
                hidden = 'UseSvcAccountCreds'
            }
            @{
                name = 'nr_of_sessions'
                type = 'textbox'
                label = 'Max. number of simultaneous sessions'
                tooltip = ''
                value = 5
            }
            @{
                name = 'sessions_idle_timeout'
                type = 'textbox'
                label = 'Session cleanup idle time (minutes)'
                tooltip = ''
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
    param (
        [switch] $Force
    )

    if (!$Force -and $Global:SqlInfoCache.Ts -and ((Get-Date) - $Global:SqlInfoCache.Ts).TotalMilliseconds -le [Int32]600000) {
        return
    }

    # Refresh cache
    $sql_command = New-OracleSqlCommand "
        SELECT
            AX.OWNER || '.' || AX.OBJECT_NAME AS full_object_name,
            (CASE WHEN AO.OBJECT_TYPE = 'TABLE' THEN 'Table' WHEN AO.OBJECT_TYPE = 'VIEW' THEN 'View' ELSE 'Other' END) AS object_type,
            ATC.COLUMN_NAME,
            (CASE WHEN PK.COLUMN_NAME      IS NULL THEN 0 ELSE 1 END) AS is_primary_key,
            (CASE WHEN ATC.IDENTITY_COLUMN = 'NO'  THEN 0 ELSE 1 END) AS is_identity,
            (CASE WHEN ATC.VIRTUAL_COLUMN  = 'NO'  THEN 0 ELSE 1 END) AS is_computed,
            (CASE WHEN ATC.NULLABLE        = 'N'   AND ATC.DATA_DEFAULT IS NULL THEN 0 ELSE 1 END) AS is_nullable
        FROM
            (
                SELECT
                    OWNER,
                    TABLE_NAME AS OBJECT_NAME
                FROM
                    SYS.ALL_TABLES
                UNION
                SELECT
                    OWNER,
                    VIEW_NAME AS OBJECT_NAME
                FROM
                    SYS.ALL_VIEWS
            ) AX
            INNER JOIN SYS.ALL_OBJECTS  AO  ON AX.OWNER = AO.OWNER  AND AX.OBJECT_NAME = AO.OBJECT_NAME
            INNER JOIN SYS.ALL_TAB_COLS ATC ON AX.OWNER = ATC.OWNER AND AX.OBJECT_NAME = ATC.TABLE_NAME
            LEFT JOIN (
                SELECT
                    ACC.*
                FROM
                    SYS.ALL_CONS_COLUMNS ACC
                    INNER JOIN SYS.ALL_CONSTRAINTS AC ON ACC.OWNER = AC.OWNER AND ACC.TABLE_NAME = AC.TABLE_NAME AND ACC.CONSTRAINT_NAME = AC.CONSTRAINT_NAME 
                WHERE
                    AC.CONSTRAINT_TYPE = 'P'
            ) PK ON AX.OWNER = PK.OWNER AND AX.OBJECT_NAME = PK.TABLE_NAME AND ATC.COLUMN_NAME = PK.COLUMN_NAME
        WHERE
            AX.OWNER NOT IN ('APPQOSSYS', 'CTXSYS', 'DBSFWUSER', 'DBSNMP', 'DVSYS', 'GSMADMIN_INTERNAL', 'LBACSYS', 'MDSYS', 'OLAPSYS', 'ORDDATA', 'ORDSYS', 'RQSYS', 'SYS','SYSTEM', 'WMSYS', 'XDB') AND
            AX.OBJECT_NAME NOT LIKE '%$%'
        ORDER BY
            full_object_name, ATC.COLUMN_ID
    "

    $objects = New-Object System.Collections.ArrayList
    $object = @{}

    # Process in one pass
    Invoke-OracleSqlCommand $sql_command | ForEach-Object {
        if ($_.full_object_name -ne $object.full_name) {
            if ($object.full_name -ne $null) {
                $objects.Add($object) | Out-Null
            }

            $object = @{
                full_name = $_.full_object_name
                type      = $_.object_type
                columns   = New-Object System.Collections.ArrayList
            }
        }

        $object.columns.Add(@{
            name           = $_.column_name
            is_primary_key = $_.is_primary_key
            is_identity    = $_.is_identity
            is_computed    = $_.is_computed
            is_nullable    = $_.is_nullable
        }) | Out-Null
    }

    if ($object.full_name -ne $null) {
        $objects.Add($object) | Out-Null
    }

    Dispose-OracleSqlCommand $sql_command

    $Global:SqlInfoCache.Objects = $objects
    $Global:SqlInfoCache.Ts = Get-Date
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

    if ($Class -eq '') {

        if ($GetMeta) {
            #
            # Get all tables and views in database
            #

            Open-OracleSqlConnection $SystemParams

            Fill-SqlInfoCache -Force

            #
            # Output list of supported operations per table/view (named Class)
            #

            @(
                foreach ($object in $Global:SqlInfoCache.Objects) {
                    $primary_keys = $object.columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name }

                    if ($object.type -ne 'Table') {
                        # Non-tables only support 'Read'
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = 'R'
                        }
                    }
                    else {
                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Create'
                        }

                        [ordered]@{
                            Class = $object.full_name
                            Operation = 'Read'
                            'Source type' = $object.type
                            'Primary key' = $primary_keys -join ', '
                            'Supported operations' = "CR$(if ($primary_keys) { 'UD' } else { '' })"
                        }

                        if ($primary_keys) {
                            # Only supported if primary keys are present
                            [ordered]@{
                                Class = $object.full_name
                                Operation = 'Update'
                            }

                            [ordered]@{
                                Class = $object.full_name
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

            Fill-SqlInfoCache

            $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

            switch ($Operation) {
                'Create' {
                    @{
                        semantics = 'create'
                        parameters = @(
                            $columns | ForEach-Object {
                                @{
                                    name = $_.name;
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
                            tooltip = 'Applied SQL where-clause'
                            value = ''
                        }
                        @{
                            name = 'selected_columns'
                            type = 'grid'
                            label = 'Include columns'
                            tooltip = 'Selected columns'
                            table = @{
                                rows = @($columns | ForEach-Object {
                                    @{
                                        name = $_.name
                                        config = @(
                                            if ($_.is_primary_key) { 'Primary key' }
                                            if ($_.is_identity)    { 'Generated' }
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
                            value = @($columns | ForEach-Object { $_.name })
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
                                    name = $_.name;
                                    allowance = if ($_.is_primary_key) { 'mandatory' } else { 'optional' }
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

                'Delete' {
                    @{
                        semantics = 'delete'
                        parameters = @(
                            $columns | ForEach-Object {
                                if ($_.is_primary_key) {
                                    @{
                                        name = $_.name
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
                Fill-SqlInfoCache

                $columns = ($Global:SqlInfoCache.Objects | Where-Object { $_.full_name -eq $Class }).columns

                $Global:ColumnsInfoCache[$Class] = @{
                    primary_keys = @($columns | Where-Object { $_.is_primary_key } | ForEach-Object { $_.name })
                    identity_col = @($columns | Where-Object { $_.is_identity    } | ForEach-Object { $_.name })[0]
                }
            }

            $primary_keys = $Global:ColumnsInfoCache[$Class].primary_keys
            $identity_col = $Global:ColumnsInfoCache[$Class].identity_col

            $function_params = ConvertFrom-Json2 $FunctionParams

            # Replace $null by [System.DBNull]::Value
            $keys_with_null_value = @()
            foreach ($key in $function_params.Keys) { if ($function_params[$key] -eq $null) { $keys_with_null_value += $key } }
            foreach ($key in $keys_with_null_value) { $function_params[$key] = [System.DBNull]::Value }

            $sql_command1 = New-OracleSqlCommand

            $projection = if ($function_params['selected_columns'].count -eq 0) { '*' } else { @($function_params['selected_columns'] | ForEach-Object { """$_""" }) -join ', ' }

            switch ($Operation) {
                'Create' {
                    if ($identity_col) {
                        $sql_command1.CommandText = "
                            BEGIN
                                DBMS_OUTPUT.ENABLE;
                                DECLARE nim_identity_col $($Class).""$identity_col""%TYPE;
                                BEGIN
                                    INSERT INTO $Class (
                                        " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                                    )
                                    VALUES (
                                        $(@($function_params.Keys | ForEach-Object { AddParam-OracleSqlCommand $sql_command1 $function_params[$_] }) -join ', ')
                                    )
                                    RETURNING
                                        ""$identity_col""
                                    INTO
                                        nim_identity_col;
                                    DBMS_OUTPUT.PUT_LINE(nim_identity_col);
                                    DBMS_OUTPUT.GET_LINE(:buffer, :status);
                                END;
                            END;
                        "

                        $p_buffer = New-Object Oracle.ManagedDataAccess.Client.OracleParameter(":buffer", [Oracle.ManagedDataAccess.Client.OracleDbType]::VarChar2, 32767, "", [System.Data.ParameterDirection]::Output)
                        $p_status = New-Object Oracle.ManagedDataAccess.Client.OracleParameter(":status", [Oracle.ManagedDataAccess.Client.OracleDbType]::Decimal,             [System.Data.ParameterDirection]::Output)

                        $sql_command1.Parameters.Add($p_buffer) | Out-Null
                        $sql_command1.Parameters.Add($p_status) | Out-Null

                        $deparam_command = DeParam-OracleSqlCommand $sql_command1

                        LogIO info 'INSERT' -In -Command $deparam_command

                        Invoke-OracleSqlCommand $sql_command1 $deparam_command

                        if ($p_status.Value.ToInt32() -ne 0) {
                            $message = "Status $($p_status.Value.ToInt32()) returned by command: $deparam_command"
                            Log error "Failed: $message"
                            Write-Error $message
                        }

                        $sql_command2 = New-OracleSqlCommand

                        $filter = """$identity_col"" = $(AddParam-OracleSqlCommand $sql_command2 $p_buffer.Value)"
                    }
                    else {
                        $sql_command1.CommandText = "
                            INSERT INTO $Class (
                                " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                            )
                            VALUES (
                                $(@($function_params.Keys | ForEach-Object { AddParam-OracleSqlCommand $sql_command1 $function_params[$_] }) -join ', ')
                            )
                        "

                        $deparam_command = DeParam-OracleSqlCommand $sql_command1

                        LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                        Invoke-OracleSqlCommand $sql_command1 $deparam_command

                        $sql_command2 = New-OracleSqlCommand

                        $filter = if ($primary_keys) {
                            @($primary_keys | ForEach-Object { """$_"" = $(AddParam-OracleSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '
                        }
                        else {
                            @($function_params.Keys | ForEach-Object { """$_"" = $(AddParam-OracleSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '
                        }
                    }

                    # Do not process
                    $sql_command1.CommandText = ""

                    $sql_command2.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-OracleSqlCommand $sql_command2

                    # Log output
                    $rv = Invoke-OracleSqlCommand $sql_command2 $deparam_command | ForEach-Object { $_ }
                    LogIO info 'INSERT' -Out $rv

                    $rv

                    Dispose-OracleSqlCommand $sql_command2
                    break
                }

                'Read' {
                    $filter = if ($function_params['where_clause'].length -eq 0) { '' } else { " WHERE $($function_params['where_clause'])" }

                    $sql_command1.CommandText = "
                        SELECT
                            $projection
                        FROM
                            $Class$filter
                    "
                    break
                }

                'Update' {
                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-OracleSqlCommand $sql_command1 $function_params[$_])" }) -join ' AND '

                    $sql_command1.CommandText = "
                        UPDATE
                            $Class
                        SET
                            " + (@($function_params.Keys | ForEach-Object { if ($_ -notin $primary_keys) { """$_"" = $(AddParam-OracleSqlCommand $sql_command1 $function_params[$_])" } }) -join ', ') + "
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-OracleSqlCommand $sql_command1

                    LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                    Invoke-OracleSqlCommand $sql_command1 $deparam_command

                    $sql_command2 = New-OracleSqlCommand

                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-OracleSqlCommand $sql_command2 $function_params[$_])" }) -join ' AND '

                    # Do not process
                    $sql_command1.CommandText = ""

                    $sql_command2.CommandText = "
                        SELECT
                            " + (@($function_params.Keys | ForEach-Object { """$_""" }) -join ', ') + "
                        FROM
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "

                    $deparam_command = DeParam-OracleSqlCommand $sql_command2

                    # Log output
                    $rv = Invoke-OracleSqlCommand $sql_command2 $deparam_command | ForEach-Object { $_ }
                    LogIO info 'UPDATE' -Out $rv

                    $rv

                    Dispose-OracleSqlCommand $sql_command2
                    break
                }

                'Delete' {
                    $filter = @($primary_keys | ForEach-Object { """$_"" = $(AddParam-OracleSqlCommand $sql_command1 $function_params[$_])" }) -join ' AND '

                    $sql_command1.CommandText = "
                        DELETE
                            $Class
                        WHERE
                            $filter AND
                            ROWNUM = 1
                    "
                    break
                }
            }

            if ($sql_command1.CommandText) {
                $deparam_command = DeParam-OracleSqlCommand $sql_command1

                LogIO info ($deparam_command -split ' ')[0] -In -Command $deparam_command

                if ($Operation -eq 'Read') {
                    # Streamed output
                    Invoke-OracleSqlCommand $sql_command1 $deparam_command
                }
                else {
                    # Log output
                    $rv = Invoke-OracleSqlCommand $sql_command1 $deparam_command | ForEach-Object { $_ }
                    LogIO info ($deparam_command -split ' ')[0] -Out $rv

                    $rv
                }
            }

            Dispose-OracleSqlCommand $sql_command1

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


function AddParam-OracleSqlCommand {
    param (
        [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand,
        $Param
    )

    $param_name = ":param$($SqlCommand.Parameters.Count)_"
    $param_value = if ($Param -isnot [system.array]) { $Param } else { $Param | ConvertTo-Json -Compress -Depth 32 }

    $SqlCommand.Parameters.Add($param_name, $param_value) | Out-Null

    return $param_name
}


function DeParam-OracleSqlCommand {
    param (
        [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand
    )

    $deparam_command = $SqlCommand.CommandText

    foreach ($p in $SqlCommand.Parameters) {
        if ($p.Direction -eq [System.Data.ParameterDirection]::Output) {
            continue
        }

        $value_txt = 
            if ($p.Value -eq [System.DBNull]::Value) {
                'NULL'
            }
            else {
                switch ($p.SqlDbType) {
                    { $_ -in @(
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::Char
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::Date
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::NChar
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::NVarChar
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::NVarChar2
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStamp
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStampLTZ
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::TimeStampTZ
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::VarChar
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::VarChar2
                        [Oracle.ManagedDataAccess.Client.OracleDbType]::XmlType
                    )} {
                        "'" + $p.Value.ToString().Replace("'", "''") + "'"
                        break
                    }
        
                    default {
                        $p.Value.ToString().Replace("'", "''")
                        break
                    }
                }
            }

        $deparam_command = $deparam_command.Replace($p.ParameterName, $value_txt)
    }

    # Make one single line
    @($deparam_command -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ -ne '' }) -join ' '
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
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $obj.$column_name = if ($data_reader[$column_name] -is [System.DBNull]) { $null } else { $data_reader[$column_name] }
                }

                # Output data
                $obj
            }

        }

        $data_reader.Close()
    }

    # Streaming
    function Invoke-OracleSqlCommand-ExecuteReader00 {
        param (
            [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand
        )

        $data_reader = $SqlCommand.ExecuteReader()
        $column_names = @($data_reader.GetSchemaTable().ColumnName)

        if ($column_names) {

            # Initialize result
            $hash_table = [ordered]@{}

            for ($i = 0; $i -lt $column_names.Count; $i++) {
                $hash_table[$column_names[$i]] = ''
            }

            $result = New-Object -TypeName PSObject -Property $hash_table

            # Read data
            while ($data_reader.Read()) {
                foreach ($column_name in $column_names) {
                    $result.$column_name = $data_reader[$column_name]
                }

                # Output data
                $result
            }

        }

        $data_reader.Close()
    }

    # Non-streaming (data stored in $data_set)
    function Invoke-OracleSqlCommand-DataAdapter-DataSet {
        param (
            [Oracle.ManagedDataAccess.Client.OracleCommand] $SqlCommand
        )

        $data_adapter = New-Object Oracle.ManagedDataAccess.Client.OracleDataAdapter($SqlCommand)
        $data_set     = New-Object System.Data.DataSet
        $data_adapter.Fill($data_set) | Out-Null

        # Output data
        $data_set.Tables[0]

        $data_set.Dispose()
        $data_adapter.Dispose()
    }

    if (! $DeParamCommand) {
        $DeParamCommand = DeParam-OracleSqlCommand $SqlCommand
    }

    Log debug $DeParamCommand

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
