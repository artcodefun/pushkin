package http

import (
	"context"
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
)

type Server struct {
	server *http.Server
}

func NewServer(router *gin.Engine, address string) *Server {
	return &Server{server: &http.Server{Addr: address, Handler: router}}
}

func (s *Server) Serve() error {
	if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func (s *Server) Shutdown(ctx context.Context) error {
	return s.server.Shutdown(ctx)
}
