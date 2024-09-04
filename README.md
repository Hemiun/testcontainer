# Test containers
## Introduction
Package that makes it simple to create and clean up container-based dependencies for automated integration/smoke tests.
For implementation used  [testcontainers-go](https://github.com/testcontainers/testcontainers-go) library.  
Implemented containers for postgres and apache kafka (KRaft mode)
## Features 
### Postgresql
- Starting and stopping a container
- Database preparation (database creation, users creation, schema creation)
- applying all up migrations

### Kafka
- Starting and stopping a container (kafka broker). A new network is created for each instance of the KafkaContainer. This avoids concurrent tests.
- Custom strategy for kafka broker readiness check (through metadata acquisition) implemented

## Installation
```
 go get github.com/Hemiun/testcontainer
```
## Getting Started
 1. Copy "scripts" folder to your project
Or create your own struct. In this case you must set path variables
```
testcontainer.CreateDatabasePath = "..."
testcontainer.CreateSchemaPath = "..."
testcontainer.MigrationPath = "..."
```

2. By default, folder script/migrations is empty and migrations are not applied (param **ApplyMigrations == false**).
You can put your migrations to this folder and change param ApplyMigrations
```
testcontainer.ApplyMigrations = true
```
3. By default, we use [testcontainers-go](https://github.com/testcontainers/testcontainers-go). If your prefer another tool your can implement your own MigrationApplyFn function
For example,  for [goose](https://github.com/pressly/goose) should be something like this:  

```go
testcontainer.MigrationApplyFn  = func(dsn string) error {
        db, err = sql.Open("postgres", dsn)
        if err != nil {
           panic(err)
        }
        defer db.Close()
        goose.SetBaseFS(embedMigrations)
        if err := goose.SetDialect("postgres"); err != nil {
            panic(err)
        }
        if err := goose.Up(db, path.Clean(buildPath(migrationPath))); err != nil {
            return err
        }
		return nil
   }
```
4. Using
```go
    db, err := testcontainer.NewDatabaseContainer(ctx, postgresConfig, nil)
	if err != nil {
		fmt.Printf("can't init db container: %v", err)
	}
	err = db.PrepareDB(ctx)
	if err != nil {
		panic("can't prepare db")
	}
	defer db.Close(ctx)
	dsn := db.ConnectionString(ctx)

	//  your database is ready/ Use dsn for connect
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close(context.Background())

	var num int64
	err = conn.QueryRow(context.Background(), "select 10").Scan(&num)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(num)
```