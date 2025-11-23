package models

type Role string

const (
	RoleAdmin Role = "admin"
	RoleUser  Role = "user"
)

type User struct {
	ID       string
	Username string
	IsActive bool
	Role     Role
}
