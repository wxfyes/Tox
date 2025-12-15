# VLESS Vision + Nginx Fallback Deployment Notes

## 1. Overview
Successful deployment of a VLESS Vision node with reliable Nginx fallback.
The setup ensures high performance (XTLS-Vision), stealth (Masquerade), and stability (Fallback).

**Protocol Stack**: `VLESS` + `TCP` + `XTLS-Vision` + `TLS`
**Fallback Target**: Nginx @ `127.0.0.1:8080` (Snake Game Masquerade)

---

## 2. Key Issues & Fixes

### A. Fallback Connectivity (Err 502 / Empty Response)
*   **Symptom**: Accessing the domain via browser returned 502 or empty response, while VLESS worked partially.
*   **Root Cause 1 (ALPN Mismatch)**: Browser negotiates `h2` (HTTP/2) over TLS, but Nginx fallback on backend expects `http/1.1`.
    *   **Fix**: Modified `core/sing/node.go` to enforce `tls.ALPN = []string{"http/1.1"}` for all VLESS-TCP nodes. This forces the entire chain to use HTTP/1.1, avoiding the protocol version mismatch.
*   **Root Cause 2 (Traffic Limiter Blocking)**: V2bX's internal hook (`HookServer.RoutedConnection`) was rejecting fallback traffic because it lacked a User UUID (`m.User` is empty for fallback).
    *   **Fix**: Modified `core/sing/hook.go`. Added a check `if m.User != ""` before applying limit checks. Fallback traffic (empty user) is now strictly bypassed/allowed.

### B. V2bX Script & Infrastructure
*   **Nginx Installation**: Updated `V2bX.sh` and `install.sh` to install Nginx listening on port `8080` (to avoid conflict with V2bX on 80/443).
*   **V2bX Status Display**: Fixed `show_menu` in `V2bX.sh` to correctly display the running status of the V2bX service at the bottom of the menu.
*   **Masquerade Page**: Replaced the default simplified HTML with a single-file **Snake Game** (Javascript) for better camouflage and interactivity.

---

## 3. Configuration Details

### V2bX Panel Config
*   **Type**: `VLESS`
*   **Port**: `443`
*   **Transport**: `TCP`
*   **Flow**: `xtls-rprx-vision`
*   **TLS**: `Enable` (Keys managed by V2bX/Certbot)
*   **Fallback**: `127.0.0.1:8080` (Implicitly handled by code logic now)

### Nginx Config (`/etc/nginx/nginx.conf`)
Listens on `127.0.0.1:8080`.
Serves `/usr/share/nginx/html/index.html` (Snake Game).

---

## 4. Verification Steps
1.  **VLESS Connectivity**: Connect via v2rayN/FlClash using Vision flow. -> **Success**.
2.  **Browser Access**: Visit `https://your-domain.com`. -> **Shows Snake Game (HTTP 200)**.
3.  **Status Check**: Run `v2bx` menu. -> **Shows "V2bX 状态: 已启动"**.

## 5. Future Maintenance
*   **Update Script**: `wget -N https://raw.githubusercontent.com/wangn9900/V2bX-script/master/install.sh && bash install.sh`
*   **Reset Masquerade**: Run `v2bx` -> Option `17` (Install/Reset Nginx).

---
*Date: 2025-12-15*
