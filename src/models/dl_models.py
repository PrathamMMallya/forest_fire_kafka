# src/models/dl_models.py
import torch
import torch.nn as nn

class LSTMModel(nn.Module):
    def __init__(self, input_size, hidden=128, layers=2):
        super().__init__()
        self.lstm = nn.LSTM(input_size, hidden, layers, batch_first=True, dropout=0.3)
        self.fc = nn.Linear(hidden, 1)
        self.sig = nn.Sigmoid()

    def forward(self, x):
        # x: (B, T, F) — MUST BE float32
        x = x.float()  # ← CRITICAL: Convert from float64 to float32
        out, _ = self.lstm(x)
        return self.sig(self.fc(out[:, -1])).squeeze(-1)

class ConvLSTMCell(nn.Module):
    def __init__(self, in_c, hidden, k=3):
        super().__init__()
        pad = k // 2
        self.conv = nn.Conv2d(in_c + hidden, 4*hidden, k, padding=pad)

    def forward(self, x, state):
        h, c = state
        combined = torch.cat([x, h], dim=1)
        gates = self.conv(combined).chunk(4, dim=1)
        i, f, o, g = gates
        i, f, o, g = torch.sigmoid(i), torch.sigmoid(f), torch.sigmoid(o), torch.tanh(g)
        c_next = f * c + i * g
        h_next = o * torch.tanh(c_next)
        return h_next, c_next

    @staticmethod
    def init_hidden(batch_size, shape, device):
        h, w = shape
        return (torch.zeros(batch_size, hidden, h, w, device=device),
                torch.zeros(batch_size, hidden, h, w, device=device))

class ConvLSTM(nn.Module):
    def __init__(self, in_c, hidden=[64, 32], k=3):
        super().__init__()
        self.cells = nn.ModuleList([
            ConvLSTMCell(in_c if i == 0 else hidden[i-1], h, k)
            for i, h in enumerate(hidden)
        ])
        self.out = nn.Conv2d(hidden[-1], 1, 1)
        self.sig = nn.Sigmoid()

    def forward(self, x):
        # x: (B, T, C, H, W) — already float32 from DataLoader
        b, t, c, h, w = x.shape
        device = x.device
        states = [None] * len(self.cells)
        for timestep in range(t):
            inp = x[:, timestep]
            for i, cell in enumerate(self.cells):
                if states[i] is None:
                    states[i] = cell.init_hidden(b, (h, w), device)
                states[i] = cell(inp, states[i])
                inp = states[i][0]
        return self.sig(self.out(states[-1][0])).squeeze(1)