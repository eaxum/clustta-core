package auth

// User represents an authenticated user's profile information.
// This is the lightweight auth user, distinct from the full database models.User.
type User struct {
	Id        string `db:"id" json:"id"`
	Username  string `db:"username" json:"username"`
	Email     string `db:"email" json:"email"`
	FirstName string `db:"first_name" json:"first_name"`
	LastName  string `db:"last_name" json:"last_name"`
	Photo     []byte `db:"photo" json:"photo"`
}

// Token holds the session identifier and user profile for an authenticated session.
type Token struct {
	SessionId string `json:"session_id"`
	User      User   `json:"user"`
}

// Provider defines the interface for authentication operations.
// Each repo (client, server, studio) implements this with its own logic.
type Provider interface {
	GetActiveUser() (User, error)
	GetToken() (Token, error)
}
