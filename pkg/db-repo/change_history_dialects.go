package db_repo

import (
	"fmt"
	"strings"

	"github.com/justtrackio/gosoline/pkg/db"
)

type statementFactory func(originalTable, historyTable *tableMetadata, changeAuthoField string) []string

var triggerStatementFactory = map[string]statementFactory{
	db.DriverMysql:  buildMySqlTriggers,
	db.DriverSqlite: buildSqliteTriggers,
}

func buildMySqlTriggers(originalTable, historyTable *tableMetadata, changeAuthorField string) []string {
	const NewRecord = "NEW"
	const OldRecord = "OLD"

	statements := []string{
		fmt.Sprintf(`CREATE TRIGGER %s_ai AFTER INSERT ON %s FOR EACH ROW %s WHERE %s`,
			originalTable.tableName,
			originalTable.tableNameQuoted,
			insertHistoryEntry(originalTable, historyTable, "insert", changeAuthorField, true),
			primaryKeysMatchCondition(originalTable, NewRecord),
		),
		fmt.Sprintf(`CREATE TRIGGER %s_au AFTER UPDATE ON %s FOR EACH ROW %s WHERE %s AND (%s)`,
			originalTable.tableName,
			originalTable.tableNameQuoted,
			insertHistoryEntry(originalTable, historyTable, "update", changeAuthorField, true),
			primaryKeysMatchCondition(originalTable, NewRecord),
			rowUpdatedCondition(originalTable, changeAuthorField),
		),
		fmt.Sprintf(`CREATE TRIGGER %s_bd BEFORE DELETE ON %s FOR EACH ROW %s WHERE %s`,
			originalTable.tableName,
			originalTable.tableNameQuoted,
			insertHistoryEntry(originalTable, historyTable, "delete", changeAuthorField, false),
			primaryKeysMatchCondition(originalTable, OldRecord),
		),
		fmt.Sprintf(`CREATE TRIGGER %s_revai BEFORE INSERT ON %s FOR EACH ROW %s`,
			historyTable.tableName,
			historyTable.tableNameQuoted,
			incrementRevision(originalTable, historyTable),
		),
	}

	return statements
}

func insertHistoryEntry(originalTable *tableMetadata, historyTable *tableMetadata, action, changeAuthorField string, includeAuthorEmail bool) string {
	columnNames := originalTable.columnNamesQuoted()
	if !includeAuthorEmail {
		columnNames = originalTable.columnNamesQuotedExcludingValue(changeAuthorField)
	}

	columns := strings.Join(columnNames, ",")
	values := "d." + strings.Join(columnNames, ", d.")

	return fmt.Sprintf(`
		INSERT INTO %s (change_history_action,change_history_revision,change_history_action_at,%s) 
			SELECT '%s', NULL, NOW(), %s 
			FROM %s AS d`,
		historyTable.tableNameQuoted,
		columns,
		action,
		values,
		originalTable.tableNameQuoted)
}

func primaryKeysMatchCondition(originalTable *tableMetadata, record string) string {
	var conditions []string
	for _, columnName := range originalTable.primaryKeyNamesQuoted() {
		condition := fmt.Sprintf("d.%s = %s.%s", columnName, record, columnName)
		conditions = append(conditions, condition)
	}
	return strings.Join(conditions, " AND ")
}

func rowUpdatedCondition(originalTable *tableMetadata, changeAuthorField string) string {
	columnNames := originalTable.columnNamesQuotedExcludingValue(changeAuthorField, ColumnUpdatedAt)
	var conditions []string
	for _, columnName := range columnNames {
		condition := fmt.Sprintf("NOT (OLD.%s <=> NEW.%s)", columnName, columnName)
		conditions = append(conditions, condition)
	}
	return strings.Join(conditions, " OR ")
}

func incrementRevision(originalTable *tableMetadata, historyTable *tableMetadata) string {
	return fmt.Sprintf(`
		BEGIN 
			SET NEW.change_history_revision = (SELECT IFNULL(MAX(d.change_history_revision), 0) + 1 FROM %s as d WHERE %s); 
		END`,
		historyTable.tableNameQuoted,
		primaryKeysMatchCondition(originalTable, "NEW"),
	)
}

func buildSqliteTriggers(originalTable, historyTable *tableMetadata, changeAuthorField string) []string {
	// TODO
	return []string{}
}
