# CAN Concatenator High Level Analyzer for Saleae Logic 2 software. This script
# concatenates Logic 2 CAN "frames" (actually individual fields of the message)
# into a single frame which can be more easily read.

# https://support.saleae.com/extensions/high-level-analyzer-extensions

from saleae.analyzers import HighLevelAnalyzer, AnalyzerFrame, ChoicesSetting, NumberSetting
from saleae.data import GraphTimeDelta
import time
from datetime import datetime

# High level analyzers must subclass the HighLevelAnalyzer class.
class CanConcatenator(HighLevelAnalyzer):
    # List of settings that a user can set for this High Level Analyzer.
    # Define settings as CLASS attributes so Logic can validate and provide values
    my_choices_setting = ChoicesSetting(choices=('Normal', 'Notched'), label = 'Mode')
    my_notched_setting = NumberSetting(label='Period(ms)')
    result_types = {
        'canframe': {
            'format': '{{data.datastring}}'
        }
    }

    def __init__(self):
        self.currentStart = 0
        self.currentId = 0
        self.currentData = b''
        self.currentCrc = 0
        self.startTime = time.time()  # Get the current time
        # Track IDs and their last seen data content
        self.seen_id_data = {}  # Maps ID to last seen data content
        # Track last transmission time for each ID (for notched mode)
        self.last_transmission_time = {}  # Maps ID to last transmission time

    def decode(self, frame: AnalyzerFrame):

        # Handle frame types separately
        if frame.type == 'identifier_field':
            self.currentStart = frame.start_time
            self.currentId = frame.data['identifier']
            # Reset data collection for this frame
            self.currentData = b''
            self.currentCrc = 0
            return None

        if frame.type == 'data_field':
            self.currentData = self.currentData + frame.data['data']
            return None
        elif frame.type == 'crc_field':
            self.currentCrc = frame.data['crc']
            return None
        elif frame.type == 'ack_field':
            # Convert frame time to seconds for comparison
            current_time_seconds = time.time()
            
            # Check if this ID has been seen before and if the data content has changed
            previous_data = None
            if self.currentId in self.seen_id_data:
                previous_data = self.seen_id_data[self.currentId]
                # If data is the same as previously seen, skip this frame
                if previous_data == self.currentData:
                    return None
            
            # Check notched mode timing filter
            if self.my_choices_setting == 'Notched':
                if self.currentId in self.last_transmission_time:
                    time_since_last_ms = (current_time_seconds - self.last_transmission_time[self.currentId]) * 1000
                    # If less time has passed than the notched period, skip this frame
                    if time_since_last_ms < self.my_notched_setting:
                        return None
                
                # Update the last transmission time for this ID
                self.last_transmission_time[self.currentId] = current_time_seconds
            
            # Update the stored data for this ID
            self.seen_id_data[self.currentId] = self.currentData
            
            # Build the concatenated representation with color highlighting for changed bytes
            datastring_plain = (
                '{:03X}'.format(self.currentId) + '#'
                + '.'.join('{:02X}'.format(a) for a in self.currentData)
            )
            
            # Build colored version for terminal output
            datastring_colored = '{:03X}'.format(self.currentId) + '#'
            data_parts = []
            for i, byte_val in enumerate(self.currentData):
                byte_str = '{:02X}'.format(byte_val)
                # If we have previous data and this byte changed, color it red
                if previous_data and i < len(previous_data) and previous_data[i] != byte_val:
                    data_parts.append('\033[91m' + byte_str + '\033[0m')  # Red color
                elif previous_data and i >= len(previous_data):
                    data_parts.append('\033[92m' + byte_str + '\033[0m')  # Green for new bytes
                else:
                    data_parts.append(byte_str)  # Normal color
            datastring_colored += '.'.join(data_parts)
            
            # Use plain datastring for analyzer frame, colored for terminal
            datastring = datastring_plain
            # If streaming to the terminal, this will be printed with colors
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] {datastring_colored}")
            return AnalyzerFrame('canframe', self.currentStart, frame.end_time, {
                'timestamp': timestamp,
                'id': self.currentId,
                'data': self.currentData,
                'crc': self.currentCrc,
                'datastring': datastring
            })

        # For all other frame types, do nothing
        return None