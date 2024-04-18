# ETL Task

### Environment

- `Python` : 3.11.6
- `Java` : 18.0.2.1
- `Apache Spark` : 3.5.1
- `Apache Hadoop` : 3.x

### Data

- Dữ liệu 1 file là ghi lại hành động 1 ngày của các user được lưu dưới dạng `json`. 
  
- Ta có 30 file thể hiện hoạt động trong 30 ngày của các user về các hoạt động xem các kênh trực tuyến thuộc thể loại nào.

### Task

- Thực hiện `ETL` (Extract - Transform - Load) dữ liệu 30 ngày

- Input : Contract , AppName , TotalDuration 

- Output : Contract , TVDuration , MovieDuration , ChildDuration , RelaxDuration , SportDuration 

- Có 2 hướng làm : 

  - Hướng 1 : Đọc dữ liệu của 30 file , sau đó mới tính toán 

  - Hướng 2 : Đọc và xử lý từng file , sau đó gộp tất cả kết quả lại và group by sum 