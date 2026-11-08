-- Mảng 2 · Truy vấn kiểm tra trên máy dữ liệu (chỉ đọc). Chạy bằng `make psql`.

-- K1. Luồng live còn chạy không: nến stream mới nhất mỗi cặp, phải cách hiện tại < 2 phút
SELECT source, symbol, max(minute) AS last_minute, now() - max(minute) AS tre
FROM candles
WHERE source IN ('stream', 'stream:update', 'official')
GROUP BY 1, 2 ORDER BY 1, 2;

-- K2. Các lần mất kết nối WebSocket (E4): có dòng 'proactive_24h' là xong mục E4 cuối
SELECT reason, count(*) AS so_lan, min(disconnected_at) AS dau_tien, max(disconnected_at) AS gan_nhat,
       sum(backfilled) AS tong_backfill
FROM ws_gaps GROUP BY reason ORDER BY so_lan DESC;

SELECT symbol, disconnected_at, reconnected_at, reconnected_at - disconnected_at AS thoi_gian, backfilled
FROM ws_gaps WHERE reason = 'proactive_24h' ORDER BY disconnected_at;

-- K3. Độ phủ theo ngày của từng nguồn (C5 cần ngày D đủ 1440 phút × 5 cặp = 7200 ở cả
--     stream, stream:update và official; batch có sau T+1)
SELECT source, date_trunc('day', minute) AS ngay, count(*) AS so_nen, count(DISTINCT symbol) AS so_cap
FROM candles
WHERE source IN ('stream', 'stream:update', 'official', 'batch') AND minute >= now() - interval '4 days'
GROUP BY 1, 2 ORDER BY 2, 1;

-- K4. candle_versions của ngày D (C5 · time-to-convergence): số nến và số phiên bản trung bình
SELECT date_trunc('day', minute) AS ngay, count(DISTINCT (symbol, minute)) AS so_nen,
       round(count(*)::numeric / nullif(count(DISTINCT (symbol, minute)), 0), 2) AS phien_ban_moi_nen
FROM candle_versions WHERE source = 'stream:update' AND minute >= now() - interval '4 days'
GROUP BY 1 ORDER BY 1;
