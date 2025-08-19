
  # CAN Sniffer

A High Level Analyzer extension for Saleae Logic 2 that concatenates CAN frame fields into readable messages and highlights changing data bytes for repeated CAN IDs.

## Features

- **Frame Concatenation**: Combines individual CAN frame fields (identifier, data, CRC) into a single readable format
- **Duplicate Filtering**: Automatically filters out repeated messages with identical data to reduce noise
- **Color-Coded Changes**: Highlights changing bytes in repeated CAN IDs with different colors:
  - **Red**: Bytes that changed from the previous transmission
  - **Green**: New bytes when data length increases
  - **Normal**: Unchanged bytes
- **Notched Mode**: Filters out extremely fast-changing data by ignoring CAN packets that change faster than a specified time period (default: 750ms)

## Output Format

Messages are displayed in the format: `ID#DATA.BYTES.SEPARATED.BY.DOTS`

Example:
```
123#01.02.03.04.05.06.07.08
```

When bytes change in repeated transmissions, the terminal output will show changed bytes in red and new bytes in green.

## Settings

- **Mode**: Choose between 'Normal' and 'Notched' operation
  - **Normal**: Shows all CAN frames with data changes
  - **Notched**: Additionally filters out rapidly changing data based on timing
- **Notched Period (ms)**: Time threshold in milliseconds for notched mode (default: 750ms). Frames changing faster than this period will be filtered out.

## Getting started

1. Build your extension by updating the Python files for your needs
2. Create a public Github repo and push your code 
3. Update this README
4. Open the Logic app and publish your extension
5. Create a Github release
6. Debug your hardware like you've never done before :)

  