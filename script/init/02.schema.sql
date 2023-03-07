-- 1. Commands for Database users creation. First user - schema owner, second - user for service
CREATE USER {{ .SchemaOwner}} WITH password '{{ .SchemaOwnerPass}}';
CREATE USER {{ .ServiceUser}} WITH password '{{ .ServiceUserPass}}';

-- 2. Schema creation
CREATE SCHEMA {{ .SchemaOwner}}  AUTHORIZATION {{ .SchemaOwner}};

-- 3. Grant
GRANT USAGE ON SCHEMA {{ .SchemaOwner}} TO {{ .ServiceUser}};

ALTER DEFAULT PRIVILEGES FOR USER {{ .SchemaOwner}} IN SCHEMA {{ .SchemaOwner}}  GRANT SELECT,INSERT,UPDATE,DELETE,TRUNCATE ON TABLES TO {{ .ServiceUser}};
ALTER DEFAULT PRIVILEGES FOR USER {{ .SchemaOwner}} IN SCHEMA {{ .SchemaOwner}}  GRANT USAGE ON SEQUENCES TO {{ .ServiceUser}};
ALTER DEFAULT PRIVILEGES FOR USER {{ .SchemaOwner}} IN SCHEMA {{ .SchemaOwner}}  GRANT EXECUTE ON FUNCTIONS TO {{ .ServiceUser}};

alter role {{ .ServiceUser}} set search_path = '{{ .SchemaOwner}}';