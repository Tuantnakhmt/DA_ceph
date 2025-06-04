# Thiết kế kiến trúc

![Kiến trúc hệ thống](Hinhve/flow_module.png)

**Hình: Kiến trúc hệ thống**

Kiến trúc hệ thống của em bao gồm các mô-đun thu thập, lưu trữ, xử lý, dự báo và trực quan-đánh giá dữ liệu. Các thành phần và luồng dữ liệu được mô tả trong hình trên.

---

**Luồng dữ liệu (1)**:  
Dữ liệu về cân đối kế toán được lấy từ VietStock và CafeF bằng công cụ Selenium. Bên cạnh đó, dữ liệu giá chứng khoán, khối lượng giao dịch,... cũng được thu thập từ CafeF bằng Selenium.

**Luồng dữ liệu (2)**:  
Tin tức tài chính của các mã cổ phiếu được thu thập tại CafeF sử dụng thư viện BeautifulSoup.

**Luồng dữ liệu (3)**:  
Dữ liệu thu thập và log quá trình được đưa vào mô-đun lưu trữ. Dữ liệu từ (1) được lưu vào **MinIO** – hệ thống lưu trữ mở rộng. Dữ liệu từ (2) được lưu vào **MongoDB**.

**Luồng dữ liệu (4 - 5)**:  
- Dữ liệu tài chính được xử lý (so sánh, chuyển đổi).
- Dữ liệu tin tức được xử lý ngôn ngữ tự nhiên bằng `newspaper3k` và tính điểm cảm xúc bằng **VADER Sentiment**.  
- Dữ liệu sau xử lý được tích hợp và lưu lại lên MinIO.

**Luồng dữ liệu (6 - 7)**:  
- Mô-đun dự báo sử dụng mô hình **XGBoost Regressor** để dự đoán giá đóng cửa.  
- Kết quả dự báo, độ quan trọng đặc trưng được lưu vào MinIO.  
- Mô hình được lập lịch tự động với **Airflow** để tái huấn luyện theo đánh giá đầu ra.

**Luồng dữ liệu (8 - 9)**:  
- Mô-đun trực quan hoá cho phép giám sát quá trình qua log, kết quả và biểu đồ bằng **Streamlit**.  
- Người dùng có thể cập nhật thông tin mới và hệ thống sẽ đánh giá điểm cảm xúc và cập nhật dữ liệu.
- Hệ thống hỗ trợ so sánh sai lệch phân phối giữa tập dữ liệu mới và hiện có bằng **Evidently**, xuất báo cáo `.html` lưu vào MinIO.  
- Kết quả của Evidently là điều kiện để kích hoạt tái huấn luyện mô hình.

---

📌 *Mọi quy trình đều được tích hợp để tạo thành một hệ thống dự báo và phân tích thị trường tài chính thông minh, linh hoạt và tự động.*
