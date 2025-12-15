# 工作进度与架构记录 - Sing-box 核心重构
**日期**：2025-12-15

## 1. 核心目标达成 (Status: SUCCESS)
*   **去肥增瘦**：彻底移除了 Xray 和 Hysteria 旧核心代码与依赖，V2bX 现转型为纯 Sing-box 驱动项目（Go 1.23 环境）。
*   **断流修复**：
    *   集成了您的私有仓库 `wangn9900/sing-box_mod`。
    *   实测该版本已启用 **QUIC v0.55** (比预期的 v0.54 更新)，配合最新的 `sing-anytls v0.0.11`，拥有最强的抗拥塞和防断流能力。
    *   完美解决了 TUIC/Hy2 与新版客户端（如 MOMclash）的兼容性问题。

## 2. 关键架构调整 (如何工作的)

为了让您既能“本地修代码”又能“云端自动发版”，我们建立了以下机制：

### 核心依赖逻辑
*   **go.mod 配置**：V2bX 指向了相对路径 `replace ... => ./sing-box_mod`。
*   **本地环境**：在 `V2bX/sing-box_mod` 建立了一个指向由于 `e:\GitHub\sing-box_mod` 的**软链接**。本地 `go build` 直接生效。
*   **云端环境 (CI)**：GitHub Actions 脚本已修改，构建时会自动 Checkout 您的 `wangn9900/sing-box_mod` 仓库到相应目录。

### 开发维护流程
以后由于只要修改了 **Sing-box 核心代码**：
1.  **本地修改**：在 `e:\GitHub\sing-box_mod` 目录修改代码。
2.  **推送云端**：务必运行 `git push` 把修改推送到 GitHub。
3.  **重新编译**：直接在 V2bX 触发新的 Release (Tag) 或 Build，CI 会自动拉最新的核心代码进行打包。
    *   *不再需要手动 update go.mod 或手动发 sing-box 的 release。*

## 3. 私有化分发脚本
*   **文件位置**：`e:\GitHub\V2bX\install_custom.sh`
*   **功能**：一键安装/更新 V2bX，所有下载源（Core Release, 管理脚本）均已指向您的账号 `wangn9900`。
*   **下一步**：建议 将此脚本内容更新到您的 `wangn9900/V2bX-script` 仓库的 `install.sh` 中，完成闭环。

## 4. 当前版本信息
*   **V2bX Version**: `v1.0.8` (已验证 Windows/Linux 编译通过 ✅)
*   **Core**: `sing-box_mod` (Go 1.23 PATCHED)
*   **QUIC**: `v0.55.0`
