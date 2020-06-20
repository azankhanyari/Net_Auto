from pathlib import Path
import sys

    # if your import statement is: from parent.one import bar, then:
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


sys.path.insert(0, str(Path(__file__).parent.parent.parent))
sys.path.insert(1, str(Path(__file__).parent.parent))
sys.path.insert(2, str(Path(__file__).parent))


sys.path.append(r'D:\PycharmProjects\Networks\src\data_cleaner')
sys.path.append(r'D:\PycharmProjects\Networks\src\data_fetcher')
sys.path.append(r'D:\PycharmProjects\Networks\src\Maps')

# src.path.append(r'src/data_fetcher')

print(sys.path)

