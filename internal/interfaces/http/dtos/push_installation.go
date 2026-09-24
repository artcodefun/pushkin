package dtos

type RegisterPushInstallationRequest struct {
	UserID         string `json:"user_id" binding:"required"`
	Platform       string `json:"platform" binding:"required,oneof=android ios"`
	PackageName    string `json:"package_name" binding:"required"`
	InstallationID string `json:"installation_id" binding:"required"`
	Token          string `json:"token" binding:"required"`
}
