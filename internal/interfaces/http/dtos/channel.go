package dtos

import "github.com/superman/pushkin/internal/application/queries/readmodels"

type CreateChannelRequest struct {
	ProviderID string `json:"provider_id" binding:"required,uuid"`
	Type       string `json:"type" binding:"required"`
	Key        string `json:"key" binding:"required"`
}

type ChannelResponse struct {
	ChannelID  string `json:"channel_id"`
	ProviderID string `json:"provider_id"`
	Type       string `json:"type"`
	Key        string `json:"key"`
	Status     string `json:"status"`
}

type CreateChannelResponse struct {
	ChannelID string `json:"channel_id"`
}

func NewChannelResponse(channel *readmodels.Channel) ChannelResponse {
	return ChannelResponse{
		ChannelID:  channel.ID.String(),
		ProviderID: channel.ProviderID.String(),
		Type:       string(channel.Type),
		Key:        channel.Key,
		Status:     string(channel.Status),
	}
}

type ListChannelsResponse struct {
	Channels []ChannelResponse `json:"channels"`
}

func NewListChannelsResponse(channels []readmodels.Channel) ListChannelsResponse {
	responses := make([]ChannelResponse, len(channels))
	for index := range channels {
		responses[index] = NewChannelResponse(&channels[index])
	}
	return ListChannelsResponse{Channels: responses}
}
