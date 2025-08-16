USE [ExchangeRate]
GO

CREATE OR ALTER PROCEDURE [dbo].[sp_AtualizarEstatisticasDesatualizadas]
/*
Descri��o: 
    Atualiza estat�sticas consideradas desatualizadas baseado em:
    1) Mais de 20% das linhas modificadas
    2) N�o atualizadas h� mais de 7 dias
*/
AS
BEGIN
    SET NOCOUNT ON;
    
    -- Declara��o de vari�veis
    DECLARE @SchemaName NVARCHAR(128),
            @TableName NVARCHAR(128),
            @StatsName NVARCHAR(128),
            @SQL NVARCHAR(MAX),
            @ModCount BIGINT,
            @TotalRows BIGINT,
            @LastUpdated DATETIME,
            @StatsUpdated INT = 0,
            @TotalStats INT = 0,
            @PercModificacao DECIMAL(5,2);
    
    -- Cria tabela tempor�ria para log
    IF OBJECT_ID('tempdb..#Log') IS NOT NULL
        DROP TABLE #Log;
    
    CREATE TABLE #Log (
        Tabela NVARCHAR(256),
        Estatistica NVARCHAR(256),
        Modificacoes BIGINT,
        TotalLinhas BIGINT,
        PercModificacao DECIMAL(5,2),
        UltimaAtualizacao DATETIME,
        Status NVARCHAR(50)
    );
    
    -- Cria tabela tempor�ria para estat�sticas desatualizadas
    IF OBJECT_ID('tempdb..#StatsDesatualizadas') IS NOT NULL
        DROP TABLE #StatsDesatualizadas;
    
    CREATE TABLE #StatsDesatualizadas (
        RowID INT IDENTITY(1,1),
        SchemaName NVARCHAR(128),
        TableName NVARCHAR(128),
        StatsName NVARCHAR(128),
        ModCount BIGINT,
        TotalRows BIGINT,
        LastUpdated DATETIME,
        PercModificacao DECIMAL(5,2)
    );
    
    -- Insere estat�sticas desatualizadas com c�lculo de porcentagem
    INSERT INTO #StatsDesatualizadas (
        SchemaName,
        TableName,
        StatsName,
        ModCount,
        TotalRows,
        LastUpdated,
        PercModificacao
    )
    SELECT 
        sch.name,
        obj.name,
        st.name,
        sp.modification_counter,
        sp.rows,
        sp.last_updated,
        CASE 
            WHEN sp.rows = 0 THEN 0 
            ELSE (sp.modification_counter * 100.0 / sp.rows) 
        END
    FROM sys.stats st
    INNER JOIN sys.objects obj ON st.object_id = obj.object_id
    INNER JOIN sys.schemas sch ON obj.schema_id = sch.schema_id
    CROSS APPLY sys.dm_db_stats_properties(st.object_id, st.stats_id) sp
    WHERE obj.type = 'U'
      AND (
          (sp.modification_counter > 0.2 * sp.rows) OR
          (DATEDIFF(DAY, sp.last_updated, GETDATE()) > 7)
      );
    
    -- Conta estat�sticas desatualizadas
    SELECT @TotalStats = COUNT(*) FROM #StatsDesatualizadas;
    
    -- Cursor para processar estat�sticas ordenadas por % de modifica��o
    DECLARE StatsCursor CURSOR FOR
    SELECT 
        SchemaName,
        TableName,
        StatsName,
        ModCount,
        TotalRows,
        LastUpdated,
        PercModificacao
    FROM #StatsDesatualizadas
    ORDER BY PercModificacao DESC;
    
    OPEN StatsCursor;
    FETCH NEXT FROM StatsCursor INTO @SchemaName, @TableName, @StatsName, @ModCount, @TotalRows, @LastUpdated, @PercModificacao;
    
    WHILE @@FETCH_STATUS = 0
    BEGIN
        BEGIN TRY
            SET @SQL = 'UPDATE STATISTICS [' + @SchemaName + '].[' + @TableName + '] [' + @StatsName + '] WITH ';
            
            -- Define estrat�gia de amostragem baseada no tamanho da tabela
            IF @TotalRows > 1000000
                SET @SQL = @SQL + 'SAMPLE 30 PERCENT';
            ELSE IF @TotalRows > 100000
                SET @SQL = @SQL + 'SAMPLE 50 PERCENT';
            ELSE
                SET @SQL = @SQL + 'FULLSCAN';
            
            EXEC sp_executesql @SQL;
            
            INSERT INTO #Log VALUES (
                @SchemaName + '.' + @TableName,
                @StatsName,
                @ModCount,
                @TotalRows,
                @PercModificacao,
                @LastUpdated,
                'ATUALIZADA'
            );
            
            SET @StatsUpdated = @StatsUpdated + 1;
        END TRY
        BEGIN CATCH
            INSERT INTO #Log VALUES (
                @SchemaName + '.' + @TableName,
                @StatsName,
                @ModCount,
                @TotalRows,
                @PercModificacao,
                @LastUpdated,
                'ERRO: ' + ERROR_MESSAGE()
            );
        END CATCH
        
        FETCH NEXT FROM StatsCursor INTO @SchemaName, @TableName, @StatsName, @ModCount, @TotalRows, @LastUpdated, @PercModificacao;
    END;
    
    CLOSE StatsCursor;
    DEALLOCATE StatsCursor;
    
    -- Exibe relat�rio resumido
    PRINT '============================================';
    PRINT ' RELAT�RIO DE ATUALIZA��O DE ESTAT�STICAS';
    PRINT '============================================';
    PRINT 'Total de estat�sticas verificadas: ' + CAST(@TotalStats AS VARCHAR);
    PRINT 'Estat�sticas atualizadas: ' + CAST(@StatsUpdated AS VARCHAR);
    PRINT '--------------------------------------------';
    
    -- Log detalhado (se a tabela existir)
    IF EXISTS (SELECT * FROM sys.tables WHERE name = 'LogManutencaoEstatisticas' AND type = 'U')
    BEGIN
        DECLARE @Detalhes NVARCHAR(MAX) = '';
        
        SELECT @Detalhes = @Detalhes + 
               Tabela + '.' + Estatistica + 
               ' (Modif: ' + CAST(PercModificacao AS VARCHAR(5)) + '%, ' +
               'Linhas: ' + CAST(TotalLinhas AS VARCHAR(10)) + ', ' +
               Status + ')' + CHAR(13) + CHAR(10)
        FROM #Log;
        
        INSERT INTO [dbo].[LogManutencaoEstatisticas] (
            DataExecucao,
            TabelasAtualizadas,
            Detalhes
        )
        VALUES (
            GETDATE(),
            @StatsUpdated,
            @Detalhes
        );
    END;
    
    -- Limpeza de objetos tempor�rios
    DROP TABLE #StatsDesatualizadas;
    DROP TABLE #Log;
    
    PRINT 'Manuten��o conclu�da em: ' + CONVERT(VARCHAR(20), GETDATE(), 120);
    PRINT '============================================';
END;
GO

-- Criando tabrla de log
CREATE TABLE [dbo].[LogManutencaoEstatisticas] (
    Id INT IDENTITY(1,1) PRIMARY KEY,
    DataExecucao DATETIME NOT NULL,
    TabelasAtualizadas INT,
    Detalhes NVARCHAR(MAX)
);
-- Verificando
EXEC [dbo].[sp_AtualizarEstatisticasDesatualizadas];

-- Visualizando resultado
SELECT * FROM [dbo].[LogManutencaoEstatisticas]
ORDER BY DataExecucao DESC;