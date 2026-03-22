package studio_service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/eaxum/clustta-core/constants"
	"github.com/eaxum/clustta-core/repository"
)

// StudioInfo represents metadata returned by a studio server's /studio-info endpoint.
type StudioInfo struct {
	Id     string `json:"id"`
	Name   string `json:"name"`
	Url    string `json:"url"`
	AltUrl string `json:"alt_url"`
}

// MinimalStudio represents a minimal studio record for listing purposes.
type MinimalStudio struct {
	Id     string `db:"id" json:"id"`
	Name   string `db:"name" json:"name"`
	URL    string `db:"url" json:"url"`
	AltURL string `db:"alt_url" json:"alt_url"`
	Active string `db:"active" json:"active"`
}

// StudioUserInfo represents a studio member's information.
type StudioUserInfo struct {
	Id         string `db:"id" json:"id"`
	FirstName  string `db:"first_name" json:"first_name"`
	LastName   string `db:"last_name" json:"last_name"`
	UserName   string `db:"username" json:"username"`
	Email      string `db:"email" json:"email"`
	Active     bool   `db:"active" json:"active"`
	RoleName   string `db:"role_name" json:"role_name"`
	StudioName string `db:"studio_name" json:"studio_name"`
	StudioId   string `db:"studio_id" json:"studio_id"`
	RoleId     string `db:"role_id" json:"role_id"`
	Photo      []byte `db:"photo" json:"photo"`
}

// GetStudioInfo fetches studio metadata from a studio server's /studio-info endpoint.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func GetStudioInfo(studioUrl string, prepareRequest repository.PrepareRequest) (StudioInfo, error) {
	if studioUrl == "" {
		return StudioInfo{}, fmt.Errorf("no studio URL provided")
	}

	url := studioUrl + "/studio-info"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return StudioInfo{}, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	response, err := client.Do(req)
	if err != nil {
		return StudioInfo{}, fmt.Errorf("failed to connect to studio server: %v", err)
	}
	defer response.Body.Close()

	if response.StatusCode == http.StatusOK {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return StudioInfo{}, fmt.Errorf("error reading response body: %v", err)
		}

		var info StudioInfo
		err = json.Unmarshal(body, &info)
		if err != nil {
			return StudioInfo{}, fmt.Errorf("failed to parse studio info: %v", err)
		}

		if info.Url == "" {
			info.Url = studioUrl
		}

		return info, nil
	}

	return StudioInfo{}, fmt.Errorf("failed to get studio info: status code %d", response.StatusCode)
}

// GetUserStudios fetches studios for the current user from the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func GetUserStudios(host string, prepareRequest repository.PrepareRequest) ([]MinimalStudio, error) {
	url := host + "/person/studios"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 201 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		var studios []MinimalStudio
		err = json.Unmarshal(body, &studios)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
		}
		return studios, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %s", err.Error())
	}

	return nil, fmt.Errorf("error loading studios: code - %d: body - %s", response.StatusCode, string(body))
}

// GetUserPhoto fetches a user's photo from the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func GetUserPhoto(userId, host string, prepareRequest repository.PrepareRequest) ([]byte, error) {
	url := host + "/person/" + userId + "/photo"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 200 || response.StatusCode == 201 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		if len(body) == 0 {
			return nil, nil
		}
		return body, nil
	}

	return nil, nil
}

// GetStudioUsers fetches all users for a studio from the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func GetStudioUsers(studioId, host string, prepareRequest repository.PrepareRequest) ([]StudioUserInfo, error) {
	url := host + "/studio/" + studioId + "/persons"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 201 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		var users []StudioUserInfo
		err = json.Unmarshal(body, &users)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
		}

		for i := range users {
			if users[i].Id != "" {
				photoData, err := GetUserPhoto(users[i].Id, host, prepareRequest)
				if err == nil && photoData != nil {
					users[i].Photo = photoData
				}
			}
		}

		return users, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %s", err.Error())
	}

	return nil, fmt.Errorf("error loading studio users: code - %d: body - %s", response.StatusCode, string(body))
}

// AddCollaborator adds a collaborator to a studio on the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func AddCollaborator(email, studioId, roleName, host string, prepareRequest repository.PrepareRequest) (interface{}, error) {
	url := host + "/studio/person"

	requestBody := map[string]string{
		"email":     email,
		"role_name": roleName,
		"studio_id": studioId,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("POST", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	req.Header.Set("Content-Type", "application/json")
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 201 || response.StatusCode == 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		var result interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
		}
		return result, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %s", err.Error())
	}

	return nil, fmt.Errorf("error adding collaborator: code - %d: body - %s", response.StatusCode, string(body))
}

// ChangeCollaboratorRole changes a collaborator's role on the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func ChangeCollaboratorRole(userId, studioId, roleName, host string, prepareRequest repository.PrepareRequest) (interface{}, error) {
	url := host + "/studio/person"

	requestBody := map[string]string{
		"user_id":   userId,
		"role_name": roleName,
		"studio_id": studioId,
	}

	jsonData, err := json.Marshal(requestBody)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequest("PUT", url, bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	req.Header.Set("Content-Type", "application/json")
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 201 || response.StatusCode == 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		var result interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
		}
		return result, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %s", err.Error())
	}

	return nil, fmt.Errorf("error changing collaborator role: code - %d: body - %s", response.StatusCode, string(body))
}

// RemoveCollaborator removes a collaborator from a studio on the given host.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func RemoveCollaborator(userId, studioId, host string, prepareRequest repository.PrepareRequest) (interface{}, error) {
	url := host + "/studio/person/" + studioId + "/" + userId

	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()

	if response.StatusCode == 201 || response.StatusCode == 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return nil, fmt.Errorf("error reading response body: %s", err)
		}

		var result interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal response body: %v", err)
		}
		return result, nil
	}

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, fmt.Errorf("error reading response body: %s", err.Error())
	}

	return nil, fmt.Errorf("error removing collaborator: code - %d: body - %s", response.StatusCode, string(body))
}

// GetServerVersion fetches the version string from a studio server.
func GetServerVersion(studioUrl string) (string, error) {
	if studioUrl == "" {
		return "", fmt.Errorf("no studio URL provided")
	}

	url := studioUrl + "/version"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()

	if response.StatusCode == 200 || response.StatusCode == 201 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return "", err
		}

		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return "", err
		}

		if version, ok := result["version"].(string); ok {
			return version, nil
		}
		return "", fmt.Errorf("version not found in response")
	}

	return "", fmt.Errorf("failed to get server version: status code %d", response.StatusCode)
}

// GetStudioStatus checks if a studio server is online by pinging it.
// The prepareRequest callback is used to attach auth headers to the HTTP request.
func GetStudioStatus(studioUrl string, prepareRequest repository.PrepareRequest) (string, error) {
	if studioUrl == "" {
		return "offline", nil
	}

	url := studioUrl + "/ping"

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "offline", nil
	}

	req.Header.Set("Clustta-Agent", constants.USER_AGENT)
	if prepareRequest != nil {
		prepareRequest(req)
	}

	client := &http.Client{}
	response, err := client.Do(req)
	if err != nil {
		return "offline", nil
	}
	defer response.Body.Close()

	if response.StatusCode == 201 || response.StatusCode == 200 {
		body, err := io.ReadAll(response.Body)
		if err != nil {
			return "offline", nil
		}

		var result map[string]interface{}
		err = json.Unmarshal(body, &result)
		if err != nil {
			return "offline", nil
		}

		if status, ok := result["status"].(string); ok {
			return status, nil
		}
	}

	return "offline", nil
}
