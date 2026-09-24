package dtos

import "github.com/superman/pushkin/internal/application/queries/readmodels"

type CreateMobileApplicationRequest struct {
	ProviderID  string `json:"provider_id" binding:"required,uuid"`
	Platform    string `json:"platform" binding:"required,oneof=android ios"`
	PackageName string `json:"package_name" binding:"required"`
}

type ConnectMobileApplicationProviderRequest struct {
	ProviderID string `json:"provider_id" binding:"required,uuid"`
}

type CreateMobileApplicationResponse struct {
	MobileApplicationID string `json:"mobile_application_id"`
}

type MobileApplicationResponse struct {
	MobileApplicationID string  `json:"mobile_application_id"`
	ProviderID          *string `json:"provider_id,omitempty"`
	Platform            string  `json:"platform"`
	PackageName         string  `json:"package_name"`
	Status              string  `json:"status"`
}

func NewMobileApplicationResponse(application *readmodels.MobileApplication) MobileApplicationResponse {
	response := MobileApplicationResponse{
		MobileApplicationID: application.ID.String(),
		Platform:            string(application.Platform),
		PackageName:         application.PackageName,
		Status:              string(application.Status),
	}
	if application.ProviderID != nil {
		providerID := application.ProviderID.String()
		response.ProviderID = &providerID
	}
	return response
}

type ListMobileApplicationsResponse struct {
	MobileApplications []MobileApplicationResponse `json:"mobile_applications"`
}

func NewListMobileApplicationsResponse(applications []readmodels.MobileApplication) ListMobileApplicationsResponse {
	responses := make([]MobileApplicationResponse, len(applications))
	for index := range applications {
		responses[index] = NewMobileApplicationResponse(&applications[index])
	}
	return ListMobileApplicationsResponse{MobileApplications: responses}
}
