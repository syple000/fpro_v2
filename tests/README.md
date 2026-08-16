# 测试目录约定

测试首先按运行模块隔离，再按测试层级拆分：

```text
tests/<module>/unit/
tests/<module>/integration/
tests/<module>/stress/
```

- `unit` 只验证单个类或纯函数，不启动真实外部服务；
- `integration` 验证模块内部组件和对外接口的组合；
- `stress` 验证并发、高频调用和容量边界下的正确性，不承担性能基准职责；
- 模块共享 fake 放在 `tests/<module>/fakes.py`，禁止从其他 `test_*.py` 导入测试工具；
- 只有真正跨模块的测试才直接放在 `tests/integration/`，不要把模块测试堆到 `tests/` 根目录。
