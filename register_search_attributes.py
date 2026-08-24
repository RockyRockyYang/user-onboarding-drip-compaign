import asyncio

import temporalio.api.enums.v1 as enums
import temporalio.api.operatorservice.v1 as operatorservice
from temporalio.client import Client
from temporalio.service import RPCError, RPCStatusCode

# 这个 namespace 需要哪些 search attribute,以及它们的类型。
# 以后要加新的 search attribute,在这里加一行就行——脚本本身负责"已存在的
# 跳过、只创建缺的",不用手动记哪些跑过、哪些没跑过。
SEARCH_ATTRIBUTES: dict[str, enums.IndexedValueType.ValueType] = {
    "stage": enums.IndexedValueType.INDEXED_VALUE_TYPE_KEYWORD,
}


async def main():
    """幂等地给当前 namespace 注册所需的 search attribute。

    对应 Planning.md Phase 5 里手动跑过的
    `temporal operator search-attribute create --name stage --type Keyword`——
    生产环境不会指望人手动敲这条命令,而是把它写成脚本、进 CI/CD 或者环境
    初始化流程里跑。这是 namespace 级别的基础设施状态,不是跟着每次代码
    部署走的东西,通常只在"开通一个新环境/新 namespace"的时候跑一次。

    幂等:对已经存在、类型也一致的 search attribute,ALREADY_EXISTS 会被
    当作成功跳过,不会报错;重复跑这个脚本是安全的。
    """
    client = await Client.connect("localhost:7233")

    for name, value_type in SEARCH_ATTRIBUTES.items():
        try:
            await client.operator_service.add_search_attributes(
                operatorservice.AddSearchAttributesRequest(
                    namespace=client.namespace,
                    search_attributes={name: value_type},
                )
            )
            print(f"Registered search attribute: {name}")
        except RPCError as e:
            if e.status == RPCStatusCode.ALREADY_EXISTS:
                print(f"Already registered, skipped: {name}")
            else:
                raise


if __name__ == "__main__":
    asyncio.run(main())
