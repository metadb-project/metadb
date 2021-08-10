package sqlx

type Postgres struct {
}

func (d *Postgres) TypeName() string {
	return "postgresql"
}
