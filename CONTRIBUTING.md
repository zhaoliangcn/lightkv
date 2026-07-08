# 贡献指南

感谢您对 LightKV 的兴趣！我们欢迎任何形式的贡献 —— 代码、文档、Bug 反馈、功能建议。

## 开发流程

1. Fork 本仓库并克隆到本地
2. 创建特性分支：`git checkout -b feature/your-feature`
3. 开发并本地测试
4. 提交 Pull Request

## 开发环境要求

- **编译器**：GCC 10+ / Clang 12+（需 C++20 支持）
- **构建工具**：CMake 3.16+
- **可选依赖**：LZ4（用于 SSTable 压缩）
- **测试**：可选安装 Python 3.8+ / Go 1.20+ / Node.js 18+ 以运行对应 SDK 测试

### 快速开始开发

```bash
# Debug 构建（含 AddressSanitizer 和调试符号）
mkdir build_debug && cd build_debug
cmake .. -DCMAKE_BUILD_TYPE=Debug
make -j$(nproc)

# 运行测试
ctest --output-on-failure
```

## 代码规范

### C++
- 遵循 C++20 标准
- 使用 `lightkv` 命名空间
- 头文件使用 `#pragma once`
- 类成员变量使用 `snake_case_` 后缀
- 函数名使用 `CamelCase`
- 尽量减少外部依赖
- 所有公开接口需有中文或英文注释

### Go
- 遵循 `gofmt` 格式
- 使用 `lightkv` 包名

### Python
- 遵循 PEP 8 规范
- 类型注解

### Node.js
- 遵循 ESLint 推荐配置
- 使用 async/await

## 提交信息规范

我们使用 [Conventional Commits](https://www.conventionalcommits.org/) 规范：

```
<type>: <简短描述>

<详细说明（可选）>
```

类型包括：
- `feat` — 新功能
- `fix` — 修复
- `docs` — 文档
- `refactor` — 重构
- `test` — 测试
- `chore` — 杂项（构建、CI 等）
- `perf` — 性能优化

示例：
```
feat: 实现 ZRANK/ZREVRANK 命令
fix: WAL 日志解析添加边界检查
docs: 更新 API 接口文档
```

## 测试

- 所有新功能需添加对应的测试用例
- 提交前确保 `./scripts/run_full_test.sh` 通过
- C++ 测试文件位于 `tests/` 目录
- SDK 测试位于 `clients/<lang>/tests/` 目录

### 运行完整测试

```bash
./scripts/run_full_test.sh
```

### 运行特定测试

```bash
cd build && ctest -R <test-name> --output-on-failure
```

## Pull Request 流程

1. 确保所有测试通过
2. 更新相关文档（如有 API 变更）
3. 更新 `TODO` 中的任务状态
4. 提交 PR，描述变更内容和动机
5. 等待 Code Review

## 文档

- 新增命令或 API 请同步更新：
  - [`docs/LightKV API接口文档.md`](./docs/LightKV%20API接口文档.md)
  - [`docs/Client-SDK-API-文档.md`](./docs/Client-SDK-API-文档.md)（如涉及 SDK）
  - [`docs/LightKV.md`](./docs/LightKV.md)（如涉及架构变更）
  - [`TODO`](./TODO)（更新任务状态）

## 问题报告

提交 Issue 时请包含：
- 环境信息（OS、编译器版本、构建类型）
- 复现步骤
- 预期行为与实际行为
- 相关日志或错误输出

## 许可证

贡献代码即表示您同意将您的贡献以 MIT 许可证授权。
