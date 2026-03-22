package repository

import (
	"errors"
	"reflect"

	"github.com/eaxum/clustta-core/auth"
	base_service "github.com/eaxum/clustta-core/base_service"
	error_service "github.com/eaxum/clustta-core/errors"
	"github.com/eaxum/clustta-core/repository/models"
	"github.com/eaxum/clustta-core/utils"

	"github.com/jmoiron/sqlx"
)

func GetRole(tx *sqlx.Tx, id string) (models.Role, error) {
	role := models.Role{}
	err := base_service.Get(tx, "role", id, &role)
	if err != nil {
		return models.Role{}, err
	}
	return role, nil
}

func CreateRole(
	tx *sqlx.Tx,
	id string,
	name string,
	attributes models.RoleAttributes,
) (models.Role, error) {
	role := models.Role{}

	params := map[string]interface{}{
		"id":   id,
		"name": name,
	}

	val := reflect.ValueOf(attributes)
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i).Interface()
		fieldName := utils.ToSnakeCase(field.Name)
		params[fieldName] = value
	}

	err := base_service.Create(tx, "role", params)
	if err != nil {
		return role, err
	}
	err = base_service.GetByName(tx, "role", name, &role)
	if err != nil {
		return models.Role{}, err
	}
	return role, nil
}

func UpdateRole(
	tx *sqlx.Tx,
	id string,
	name string,
	attributes models.RoleAttributes,
) (models.Role, error) {
	role := models.Role{}

	params := map[string]interface{}{
		"name": name,
	}

	val := reflect.ValueOf(attributes)
	typ := val.Type()
	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i).Interface()
		fieldName := utils.ToSnakeCase(field.Name)
		params[fieldName] = value
	}

	err := base_service.Update(tx, "role", id, params)
	if err != nil {
		return role, err
	}
	err = base_service.UpdateMtime(tx, "role", id, utils.GetEpochTime())
	if err != nil {
		return role, err
	}
	err = base_service.GetByName(tx, "role", name, &role)
	if err != nil {
		return models.Role{}, err
	}
	return role, nil
}

func DeleteRole(tx *sqlx.Tx, id string) error {
	err := base_service.Delete(tx, "role", id)
	if err != nil {
		return err
	}
	return nil
}

func GetRoles(tx *sqlx.Tx) ([]models.Role, error) {
	role := []models.Role{}
	err := base_service.GetAll(tx, "role", &role)
	if err != nil {
		return role, err
	}
	return role, nil
}

func GetRoleByName(tx *sqlx.Tx, name string) (models.Role, error) {
	role := models.Role{}
	err := base_service.GetByName(tx, "role", name, &role)
	if err != nil {
		return role, err
	}
	return role, nil
}

func GetOrCreateRole(tx *sqlx.Tx, name string,
	attributes models.RoleAttributes,
) (models.Role, error) {
	role, err := GetRoleByName(tx, name)
	if err == nil {
		return role, nil
	}
	createdRole, err := CreateRole(tx, "", name, attributes)
	if err != nil {
		return models.Role{}, err
	}
	return createdRole, nil
}

func GetUser(tx *sqlx.Tx, id string) (models.User, error) {
	user := models.User{}
	err := base_service.Get(tx, "user", id, &user)
	if err != nil && errors.Is(err, error_service.ErrUserNotFound) {
		return user, error_service.ErrUserNotFound
	} else if err != nil {
		return user, err
	}
	userRole, err := GetRole(tx, user.RoleId)
	if err != nil {
		return user, err
	}
	user.Role = userRole
	return user, nil
}

// AddUser fetches user data from the auth server and adds the user to the project.
// The authProvider is used to fetch user data by email.
func AddUser(
	tx *sqlx.Tx,
	email string,
	roleName string,
	authProvider auth.Provider,
) (models.User, error) {
	role, err := GetRoleByName(tx, roleName)
	if err != nil {
		return models.User{}, err
	}
	userData, err := authProvider.FetchUserData(email)
	if err != nil {
		if errors.Is(err, error_service.ErrNotAutheticated) {
			return models.User{}, error_service.ErrNotAutheticated
		} else if errors.Is(err, error_service.ErrNotUnauthorized) {
			return models.User{}, error_service.ErrNotUnauthorized
		}
		return models.User{}, err
	}
	user := models.User{}
	addedAt := utils.GetCurrentTime()
	params := map[string]interface{}{
		"id":         userData.Id,
		"added_at":   addedAt,
		"username":   userData.Username,
		"email":      email,
		"first_name": userData.FirstName,
		"last_name":  userData.LastName,
		"role_id":    role.Id,
	}
	err = base_service.Create(tx, "user", params)
	if err != nil {
		return user, err
	}
	err = base_service.Get(tx, "user", userData.Id, &user)
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

// AddKnownUser adds a user with known details to the project.
// If fetchPhoto is true, the authProvider is used to fetch the user's photo.
func AddKnownUser(
	tx *sqlx.Tx,
	id string,
	email string,
	username string,
	firstName string,
	lastName string,
	roleId string,
	photo []byte,
	fetchPhoto bool,
	authProvider auth.Provider,
) (models.User, error) {
	user := models.User{}
	role, err := GetRole(tx, roleId)
	if err != nil {
		return user, err
	}
	addedAt := utils.GetCurrentTime()
	userPhoto := photo
	if fetchPhoto {
		userPhoto, err = authProvider.FetchUserPhoto(id)
		if err != nil {
			return models.User{}, err
		}
	}

	params := map[string]interface{}{
		"id":         id,
		"added_at":   addedAt,
		"username":   username,
		"email":      email,
		"first_name": firstName,
		"last_name":  lastName,
		"role_id":    role.Id,
		"photo":      userPhoto,
	}
	err = base_service.Create(tx, "user", params)
	if err != nil {
		return user, err
	}
	err = base_service.Get(tx, "user", id, &user)
	if err != nil {
		return models.User{}, err
	}
	return user, nil
}

// UpdateUsersPhoto fetches and updates photos for all users in the project.
// The authProvider is used to fetch each user's photo.
func UpdateUsersPhoto(
	tx *sqlx.Tx,
	authProvider auth.Provider,
) error {
	users, err := GetUsers(tx)
	if err != nil {
		return err
	}
	for _, user := range users {
		userPhoto, err := authProvider.FetchUserPhoto(user.Id)
		if err != nil {
			return err
		}

		params := map[string]any{
			"photo": userPhoto,
		}

		base_service.Update(tx, "user", user.Id, params)
	}
	return nil
}

func GetUsers(tx *sqlx.Tx) ([]models.User, error) {
	users := []models.User{}
	err := base_service.GetAll(tx, "user", &users)
	if err != nil {
		return users, err
	}
	for i, user := range users {
		userRole, err := GetRole(tx, user.RoleId)
		if err != nil {
			return users, err
		}
		users[i].Role = userRole
	}
	return users, nil
}

func ChangeUserRoleByName(tx *sqlx.Tx, userId string, role_name string) error {
	role, err := GetRoleByName(tx, role_name)
	if err != nil {
		return err
	}
	role_id := role.Id
	err = ChangeUserRole(tx, userId, role_id)
	if err != nil {
		return err
	}
	return nil
}

func getRoleUsers(tx *sqlx.Tx, roleId string) ([]models.User, error) {
	users := []models.User{}
	conditions := map[string]interface{}{
		"role_id": roleId,
	}
	err := base_service.GetAllBy(tx, "user", conditions, &users)
	if err != nil {
		return users, err
	}
	return users, err
}

func ChangeUserRole(tx *sqlx.Tx, userId string, roleId string) error {
	params := map[string]interface{}{
		"role_id": roleId,
	}
	adminRole, err := GetRoleByName(tx, "admin")
	if err != nil {
		return err
	}
	adminUsers, err := getRoleUsers(tx, adminRole.Id)
	if err != nil {
		return err
	}

	adminIds := []string{}
	for _, adminUser := range adminUsers {
		adminIds = append(adminIds, adminUser.Id)
	}
	if utils.Contains(adminIds, userId) && roleId != adminRole.Id && len(adminUsers) <= 1 {
		return error_service.ErrMustHaveAdmin
	}
	err = base_service.Update(tx, "user", userId, params)
	if err != nil {
		return err
	}
	err = base_service.UpdateMtime(tx, "user", userId, utils.GetEpochTime())
	if err != nil {
		return err
	}
	return nil
}

// RemoveUser removes a user from the project, unassigning their assets first.
// The authProvider is used to verify the active user is not removing themselves.
func RemoveUser(tx *sqlx.Tx, userId string, authProvider auth.Provider) error {
	assets, err := GetUserAssets(tx, userId)
	if err != nil {
		return err
	}
	if len(assets) != 0 {
		assetIds := make([]string, len(assets))
		for i, asset := range assets {
			assetIds[i] = asset.Id
		}
		err = UnAssignAssets(tx, assetIds)
		if err != nil {
			return err
		}
	}
	activeUser, err := authProvider.GetActiveUser()
	if err != nil {
		return err
	}

	if activeUser.Id == userId {
		return errors.New("you cannot remove youself")
	}
	err = base_service.Delete(tx, "user", userId)
	if err != nil {
		return err
	}
	return nil
}
