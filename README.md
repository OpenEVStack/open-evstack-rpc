# ⚡ open-evstack-rpc

A lightweight, production-ready RPC server built with Express and WebSocket for handling OCPP-based EV charger communication. Part of the [OpenEVStack](https://github.com/openevstack) ecosystem.

---

## 🌐 Overview

`open-evstack-rpc` is a robust WebSocket-based RPC server designed to handle bidirectional communication between Electric Vehicle Charging Stations and Central Systems. It is the foundational transport layer for implementing OCPP 1.6 and OCPP 2.0.1 protocols.

This package is framework-agnostic and can be easily integrated into any backend Node.js application. Built with extensibility and performance in mind.

---

## 🚀 Features

- ✅ WebSocket server with custom RPC message format
- ✅ Supports request-response and push notifications
- ✅ Built-in timeout handling and retries
- ✅ Pluggable message validators
- ✅ Heartbeat & ping-pong mechanism
- ✅ Middleware-style hooks for custom events
- ✅ Type-safe architecture (written in TypeScript)

---

## 📦 Installation

```bash
npm install @openevstack/ocpp-rpc
```