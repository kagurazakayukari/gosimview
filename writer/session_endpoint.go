// This file is part of GoSimView
// Copyright (C) 2026 KagurazakaYukari
// See LICENSE.md for license information.

package main

func globalNodeID() string {
	if cfg == nil {
		return ""
	}
	return cfg.App.Server.NodeID
}
