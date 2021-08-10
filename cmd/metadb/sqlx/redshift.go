package sqlx

type Redshift struct {
}

func (d *Redshift) TypeName() string {
	return "redshift"
}
