package models

type Role string

const (
	RoleAdmin Role = "admin"
	RoleUser  Role = "user"
)

type User struct {
	ID       int
	Username string
	IsActive bool
	Role     Role
}
