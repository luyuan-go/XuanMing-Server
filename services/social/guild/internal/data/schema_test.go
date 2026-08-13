package data

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

func TestValidateRequiredSchemaMetadata(t *testing.T) {
	complete := validRequiredSchemaMetadata()
	if err := validateRequiredSchemaMetadata(complete); err != nil {
		t.Fatalf("完整 schema 被拒绝: %v", err)
	}

	tests := []struct {
		name   string
		mutate func(*requiredSchemaMetadata)
		want   string
	}{
		{
			name: "missing column",
			mutate: func(metadata *requiredSchemaMetadata) {
				delete(metadata.Columns, "guilds.pending_request_count")
			},
			want: "guilds.pending_request_count 缺失",
		},
		{
			name: "wrong data type",
			mutate: func(metadata *requiredSchemaMetadata) {
				column := metadata.Columns["guilds.pending_request_count"]
				column.DataType, column.ColumnType = "bigint", "bigint"
				metadata.Columns["guilds.pending_request_count"] = column
			},
			want: "DATA_TYPE=bigint",
		},
		{
			name: "player id must be unsigned",
			mutate: func(metadata *requiredSchemaMetadata) {
				column := metadata.Columns["player_group_counts.player_id"]
				column.ColumnType = "bigint"
				metadata.Columns["player_group_counts.player_id"] = column
			},
			want: "期望 unsigned",
		},
		{
			name: "counter must not be nullable",
			mutate: func(metadata *requiredSchemaMetadata) {
				column := metadata.Columns["player_group_counts.group_count"]
				column.Nullable = "YES"
				metadata.Columns["player_group_counts.group_count"] = column
			},
			want: "IS_NULLABLE=YES",
		},
		{
			name: "counter default must be zero",
			mutate: func(metadata *requiredSchemaMetadata) {
				column := metadata.Columns["guilds.pending_request_count"]
				column.Default = sql.NullString{String: "7", Valid: true}
				metadata.Columns["guilds.pending_request_count"] = column
			},
			want: "COLUMN_DEFAULT=7",
		},
		{
			name: "counter table primary key must be exact",
			mutate: func(metadata *requiredSchemaMetadata) {
				metadata.PlayerGroupCountsPrimary = []string{"player_id", "group_count"}
			},
			want: "PRIMARY KEY=[player_id group_count]",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			metadata := validRequiredSchemaMetadata()
			tt.mutate(&metadata)
			err := validateRequiredSchemaMetadata(metadata)
			if err == nil || !strings.Contains(err.Error(), tt.want) || !strings.Contains(err.Error(), "version=2") {
				t.Fatalf("错误=%v，期望包含 %q 和 version=2", err, tt.want)
			}
		})
	}
}

func validRequiredSchemaMetadata() requiredSchemaMetadata {
	return requiredSchemaMetadata{
		Columns: map[string]schemaColumnMetadata{
			"guilds.pending_request_count": {
				DataType: "int", ColumnType: "int", Nullable: "NO",
				Default: sql.NullString{String: "0", Valid: true},
			},
			"player_group_counts.player_id": {
				DataType: "bigint", ColumnType: "bigint unsigned", Nullable: "NO",
			},
			"player_group_counts.group_count": {
				DataType: "int", ColumnType: "int", Nullable: "NO",
				Default: sql.NullString{String: "0", Valid: true},
			},
		},
		PlayerGroupCountsPrimary: []string{"player_id"},
	}
}

func TestValidateRequiredSchemaAcrossBackends(t *testing.T) {
	forEachBackend(t, func(t *testing.T, bc backendCase) {
		// 读元数据与 DDL 分开计时:别让前者吃掉后者的预算(ddlTimeout 见 guild_repo_mysql_test.go)。
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := ValidateRequiredSchema(ctx, bc.db); err != nil {
			t.Fatalf("[%s] final schema gate: %v", bc.name, err)
		}

		ddlCtx, ddlCancel := context.WithTimeout(context.Background(), ddlTimeout)
		defer ddlCancel()
		if _, err := bc.db.ExecContext(ddlCtx, `ALTER TABLE guilds DROP COLUMN pending_request_count`); err != nil {
			t.Fatalf("[%s] 构造旧 schema: %v", bc.name, err)
		}

		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel2()
		err := ValidateRequiredSchema(ctx2, bc.db)
		if err == nil || !strings.Contains(err.Error(), "guilds.pending_request_count") {
			t.Fatalf("[%s] 旧 schema 未被启动门禁拒绝: %v", bc.name, err)
		}
	})
}
