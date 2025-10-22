from typing import Optional, Type, Union # import logging # from panda_plugins.base import BaseWorkNode, work_node, ui # from pydantic import BaseModel, Field

"""
Advanced Control for Work Node Inputs
This example demonstrates:
1. Using the UI decorator to control input widget types
2. Setting appropriate default values for different input types
3. Configuring input properties like min/max values, placeholders, etc.

Supported input types include:
- text_field: Single-line or multi-line text input
- code_editor: Specialized editor for code with syntax highlighting
- password_field: Masked input for sensitive information
- number_field: Input for numeric values
- slider: Drag control for selecting a value within a range
- switch: Toggle control for boolean values
- combobox: Dropdown with selectable options
- date_picker: Control for selecting dates
- time_picker: Control for selecting times
- datetime_picker: Control for selecting date and time
- file_picker: Interface for selecting files

工作节点输入项的高级控制
此示例演示了：
1. 使用UI装饰器控制输入控件类型
2. 为不同输入类型设置适当的默认值
3. 配置输入属性，如最小/最大值、占位符等

支持的输入类型包括：
- text_field: 单行或多行文本输入框
- code_editor: 带有语法高亮的代码编辑器
- password_field: 用于敏感信息的掩码输入框
- number_field: 数值输入框
- slider: 用于在范围内选择值的拖动控件
- switch: 用于布尔值的开关控件
- combobox: 带有可选项的下拉框
- date_picker: 日期选择控件
- time_picker: 时间选择控件
- datetime_picker: 日期和时间选择控件
- file_picker: 文件选择界面
- None: 不显示输入框, 只能连线

"""

@ui(
    text={
        "input_type": "text_field",
        "min_lines": 1,
        "max_lines": 10,
        "placeholder": "Please enter text",
    },
    text_not_allow_link={
        "input_type": "text_field",
        "min_lines": 1,
        "max_lines": 10,
        "placeholder": "Please enter text",
        "allow_link": False,  # 不能连线,只能填写
    },
    only_link={
        "input_type": "None",
    },
    code={"input_type": "code_editor"},
    password={"input_type": "password_field"},
    number={"input_type": "number_field", "placeholder": "Please enter number"},
    slider={"input_type": "slider", "min": 0, "max": 1},
    switch={"input_type": "switch"},
    combobox={
        "input_type": "combobox",
        "options": ["option1", "option2", "option3"],
        "placeholder": "Please select",
    },
    date={"input_type": "date_picker"},
    time={"input_type": "time_picker"},
    datetime={"input_type": "datetime_picker"},
    file={"input_type": "file_picker"},
)
class InputModel(BaseModel):
    text: str = Field(default="")
    text_not_allow_link: str = Field(default="")
    only_link: str = Field(default="")
    code: str = Field(
        default="__if __name__ == '__main__':\n    print('Hello, World!')"
    )
    password: str = Field(default="")
    number: Union[int, float] = Field(default=0)
    slider: float = Field(default=0.5)
    switch: bool = Field(default=False)
    combobox: str = Field(default="option1")
    date: str = Field(default="2025-01-01")
    time: str = Field(default="12:00:00")
    datetime: str = Field(default="2025-01-01 12:00:00")
    file: str = Field(default="")

@work_node(name="示例-输入项控制", group="99-测试节点", type="general", box_color="red")
class ExampleAdvancedInputControl(BaseWorkNode):

    @classmethod
    def input_model(cls) -> Optional[Type[BaseModel]]:
        return InputModel

    @classmethod
    def output_model(cls) -> Optional[Type[BaseModel]]:
        return

    def run(self, input: BaseModel) -> Optional[Type[BaseModel]]:
        return

if __name__ == "__main__":
    print(InputModel.model_json_schema())
