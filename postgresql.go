package testcontainer

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path"
	"text/template"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/go-playground/validator/v10"
	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5"
	_ "github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

//--------------

var (
	// CreateDatabasePath - path to database creation script template (see ./script/init/01.database.sql for example)
	CreateDatabasePath = "script/init/01.database.sql"

	// CreateSchemaPath  - path to database schema creation script template (see ./script/init/02.schema.sql for example)
	CreateSchemaPath = "script/init/02.schema.sql"
	// MigrationPath - path to migration scripts
	MigrationPath = "script/migrations"
	// ApplyMigrations - if false migrations doesn't apply
	ApplyMigrations = false

	// DefaultDB - default database name (used for first connection)
	DefaultDB = "postgres"
	// DefaultDBUser - database user
	DefaultDBUser = "postgres"
	// DefaultDBPass - pass for DefaultDBUser
	DefaultDBPass = "postgres"

	// ExposePostgresPort - database port for expose from container
	ExposePostgresPort = "5432/tcp"

	// PostgresImage - docker image name for postgres
	PostgresImage = "postgres:16.4"
)

// DatabaseContainerConfig - configuration for database container (postgresql)
// Usually we use the couple of users. First one is schema owner.  For example, with this user we apply database migrations.
// Second is service user. It is restricted user and have only privileges needed for service work. Service connect to database with this credentials
type DatabaseContainerConfig struct {
	DatabaseName    string        `validate:"required"`
	SchemaOwner     string        `validate:"required"`
	SchemaOwnerPass string        `validate:"required"`
	ServiceUser     string        `validate:"required"`
	ServiceUserPass string        `validate:"required"`
	Timeout         time.Duration `validate:"required"`
}

// Validate - validation for DatabaseContainerConfig
func (c *DatabaseContainerConfig) Validate() error {
	return validator.New().Struct(c)
}

type MigrationApplyFunc func(dsn string) error

var MigrationApplyFn MigrationApplyFunc = func(dsn string) error {
	m, err := migrate.New("file://"+path.Clean(MigrationPath), dsn)
	if err != nil {
		// db.logger.LogError(ctx, "can't create migrate struct", err)
		return err
	}
	err = m.Up()
	if err != nil {
		// db.logger.LogError(ctx, "can't apply migrates", err)
		return err
	}
	return nil
}

// DatabaseContainer - struct for db container
type DatabaseContainer struct {
	logger   Logger
	instance testcontainers.Container
	cfg      DatabaseContainerConfig
}

// NewDatabaseContainer  returns new DatabaseContainer
// ctx and cfg are mandatory params
// Optionally you can pass logger
func NewDatabaseContainer(ctx context.Context, cfg DatabaseContainerConfig, log Logger) (*DatabaseContainer, error) {
	var target DatabaseContainer

	target.cfg = cfg

	if log != nil {
		target.logger = log
	} else {
		target.logger = newLogger()
		testcontainers.Logger = &containerLogger{log: target.logger}
	}

	if err := target.cfg.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	// ExposePostgresPort
	// Prepare new wait strategy for postgres db
	w := wait.ForSQL(nat.Port(ExposePostgresPort), "postgres", func(host string, port nat.Port) string {
		return fmt.Sprintf("postgres://postgres:postgres@127.0.0.1:%s/postgres?sslmode=disable", port.Port())
	}).WithQuery("select 10")

	req := testcontainers.ContainerRequest{
		Image:        PostgresImage,
		ExposedPorts: []string{ExposePostgresPort},
		Env: map[string]string{
			"POSTGRES_USER":     DefaultDBUser,
			"POSTGRES_PASSWORD": DefaultDBPass,
			"POSTGRES_DB":       DefaultDB,
		},
		WaitingFor: w,
	}
	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic("can't start container")
	}

	target.instance = postgres
	return &target, nil
}

// Port - return port for db connection (looking for port mapped into default postgresql port - 5432 )
func (db *DatabaseContainer) Port(ctx context.Context) int {
	ctx, cancel := context.WithTimeout(ctx, db.cfg.Timeout)
	defer cancel()
	p, err := db.instance.MappedPort(ctx, "5432")
	if err != nil {
		db.logger.LogError(ctx, "can't get port", err)
		return 0
	}
	return p.Int()
}

// ConnectionString - returns connection string in dsn format
func (db *DatabaseContainer) ConnectionString(ctx context.Context) string {
	return db.connectionString(ctx, db.cfg.DatabaseName, db.cfg.ServiceUser, db.cfg.ServiceUserPass)
}

func (db *DatabaseContainer) connectionString(ctx context.Context, dbName string, user string, pass string) string {
	return fmt.Sprintf("postgres://%s:%s@127.0.0.1:%d/%s?sslmode=disable", user, pass, db.Port(ctx), dbName)
}

// Close - close created container
func (db *DatabaseContainer) Close(ctx context.Context) {
	ctx, cancel := context.WithTimeout(ctx, db.cfg.Timeout)
	defer cancel()
	err := db.instance.Terminate(ctx)
	if err != nil {
		db.logger.LogError(ctx, "can't close container", err)
	}
}

// PrepareDB  - prepare database structure (new db, new schema, applying migration)
func (db *DatabaseContainer) PrepareDB(ctx context.Context) error {
	err := db.createDBAndSchema(ctx)
	if err != nil {
		db.logger.LogError(ctx, "error while createDBAndSchema", err)
		return err
	}

	err = db.runMigrate(ctx)
	if err != nil {
		db.logger.LogError(ctx, "error while migration apply", err)
		return err
	}
	return nil
}

// buildScriptFromTemplate - read and processes a script from the passed path.
// It is possible to use any value from DatabaseContainerConfig struct as parameter in script.
func (db *DatabaseContainer) buildScriptFromTemplate(ctx context.Context, path string) (string, error) {
	db.logger.LogDebug(ctx, "looking fot a script:"+path)
	b, err := os.ReadFile(path)
	if err != nil {
		db.logger.LogError(ctx, "Can't read script py path:"+path, err)
		return "", err
	}
	source := string(b)
	scriptTemplate, err := template.New("db").Parse(source)
	if err != nil {
		db.logger.LogError(ctx, "Can't parse template", err)
		return "", err
	}
	buf := new(bytes.Buffer)
	err = scriptTemplate.Execute(buf, db.cfg)
	if err != nil {
		db.logger.LogError(ctx, "Can't execute template", err)
		return "", err
	}
	return buf.String(), nil
}

func (db *DatabaseContainer) createDBAndSchema(ctx context.Context) error {
	db.logger.LogDebug(ctx, "container_id:"+db.instance.GetContainerID())
	ctx, cancel := context.WithTimeout(ctx, db.cfg.Timeout)
	defer cancel()

	// first time we connect with default credentials
	dsn := db.connectionString(ctx, DefaultDB, DefaultDBUser, DefaultDBPass)
	db.logger.LogDebug(ctx, "dsn:"+dsn)
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't get connection", err)
	}

	// db creation
	script, err := db.buildScriptFromTemplate(ctx, CreateDatabasePath)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't build script from template (create database)", err)
	}
	_, err = conn.Exec(ctx, script)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't execute script:"+CreateDatabasePath, err)
	}
	//  reconnect to created db
	err = conn.Close(ctx)
	if err != nil {
		db.logger.LogError(ctx, "can't close connection", err)
	}

	// user and schema creation
	dsn = db.connectionString(ctx, db.cfg.DatabaseName, "postgres", "postgres")
	conn, err = pgx.Connect(ctx, dsn)
	defer func(conn *pgx.Conn, ctx context.Context) {
		e := conn.Close(ctx)
		if e != nil {
			db.logger.LogError(ctx, "can't close connection", e)
		}
	}(conn, ctx)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't get connection", err)
	}

	// schema object creation
	script, err = db.buildScriptFromTemplate(ctx, CreateSchemaPath)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't build script from template (create schema)", err)
	}
	db.logger.LogDebug(ctx, script)
	_, err = conn.Exec(ctx, script)
	if err != nil {
		db.logger.LogPanic(ctx, "Can't execute script:"+CreateSchemaPath, err)
	}

	return nil
}

func (db *DatabaseContainer) runMigrate(ctx context.Context) error {
	if !ApplyMigrations {
		db.logger.LogDebug(ctx, "skip applying migrations")
		return nil
	}
	dsn := db.connectionString(ctx, db.cfg.DatabaseName, db.cfg.SchemaOwner, db.cfg.SchemaOwnerPass)
	err := MigrationApplyFn(dsn)
	if err != nil {
		db.logger.LogError(ctx, "can't apply migrations", err)
		return err
	}
	return nil
}
