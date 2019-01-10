package Common;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;


/**
 * Excel文件操作帮助类
 * @author saikikky
 *
 */
public class ExcelPOIHelper {
	
	/**
	 * 文稿中创建一个空的.xls文件
	 * @param path
	 * @param name
	 */
	public static void Create(String filepath, String sheetname, Map<Integer, Map<String, String>> dataMap) {
		// 老版是HSSWorkbook()
	//	Workbook wb = new HSSFWorkbook();
		// 新版是XSSWorkbook()
		// 创建一个工作簿
		Workbook wb = new XSSFWorkbook();
		FileOutputStream fileOutputStream;
		
		try {
			fileOutputStream = new FileOutputStream(filepath);
			// 新建一个sheet对象 电子表格
			XSSFSheet sheet = (XSSFSheet) wb.createSheet(sheetname);
			// 数据插入
			if (dataMap != null) {
				InsertData(sheet, dataMap);
				wb.write(fileOutputStream);
			}
			fileOutputStream.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 往sheet里面添加数据
	 * @param sheet
	 * @param dataMap
	 */
	private static void InsertData(XSSFSheet sheet, Map<Integer, Map<String, String>> dataMap) {
		
		// 用于判定当前遍历的行数
		int index = 0;
		for (Entry<Integer, Map<String, String>> dataEntry : dataMap.entrySet()) {
			
			// index=0时为第一行,添加表头
			if (index == 0) {
				// 创建行
				XSSFRow row = sheet.createRow(index);
				Map<String, String> map = dataEntry.getValue();
				Integer rowKey = dataEntry.getKey();
				
				int cellIndex = 0;
				for (Entry<String, String> entry : map.entrySet()) {
					XSSFCell cell = row.createCell(cellIndex++);
					cell.setCellValue(entry.getKey());
				}
			}
			// 添加表数据
			XSSFRow row = sheet.createRow(++index);
			Map<String, String> map = dataEntry.getValue();
			
			int cellIndex = 0;
			for (Entry<String, String> entry : map.entrySet()) {
				XSSFCell cell = row.createCell(cellIndex++);
				cell.setCellValue(entry.getValue());
			}
		}
		
	}
	
	/**
	 * 取出Excel所有工作簿名
	 * @param fullPath Excel文件完整地址
	 * @return 工作簿名列表
	 */
	public static List<String> GetSheets(String fullPath) {
		List<String> result = new ArrayList<String>();
		try {
			FileInputStream file = new FileInputStream(fullPath);
			Workbook workbook = new XSSFWorkbook(file);
			for (int i = 0; i < workbook.getNumberOfSheets(); i++) {
				String sheetName = workbook.getSheetName(i);
				result.add(i, sheetName);
			}
			file.close();
		} catch (FileNotFoundException e) {
			e.getStackTrace();
		} catch (IOException e) {
			e.getStackTrace();
		}
		return result;
	}
	
	/**
	 * 取工作簿中所有的行
	 * @param fullPath Excel文件完整地址
	 * @param sheetName 工作簿名
	 * @return 键值对<RowKey, <ColumnName, Value>>
	 */
	public static Map<Integer, Map<String, String>> GetExcelData(String fullPath, String sheetName) {
		 Map<Integer, Map<String, String>> resultMap = new HashMap<Integer, Map<String, String>>();
		 Map<String, String> resultCell;
		 try {
			 FileInputStream file = new FileInputStream(fullPath);
			 
			 /*
			  * 旧版用法
			 POIFSFileSystem ts = new POIFSFileSystem(file);
			 Workbook workbook = new HSSFWorkbook(ts);
			 */
			 
			 Workbook workbook = new XSSFWorkbook(file);
			 // 获得工作表名
			 Sheet sheet = workbook.getSheet(sheetName);
			 
			 // 行数
			 int rowCounts = sheet.getPhysicalNumberOfRows();
			 // 列数
			 int columnCounts = sheet.getRow(0).getPhysicalNumberOfCells();
			 
			 // 第一行为工作簿名 从第二行开始
			 for (int i = 1; i < rowCounts; i++) {
				 // 循环取第一行之后的每一行
				 Row row = sheet.getRow(i);
				 row.getCell(0).setCellType(Cell.CELL_TYPE_STRING);
				 
				 resultCell = new HashMap<String, String>();
				 // 行键用行数得
				 // String rowKey = row.getCell(0).toString();
				 
				
				 for (int j = 0; j < columnCounts; j++) {
					 // 循环取第一列之后的每一列 
					 Cell cell = row.getCell(j);
					 if (null != cell) {
						 cell.setCellType(Cell.CELL_TYPE_STRING);
						 
						 String columnName = sheet.getRow(0).getCell(j).toString();
						 String cellValue = cell.toString();
						 
						 resultCell.put(columnName, cellValue);
					 }
				 }
				 resultMap.put(i, resultCell);
			 }
			 file.close();
		 } catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return resultMap;
	}
	
	
	/*
	public static void main(String[] agrs) {
		String fullPath = "/Users/saikikky/Documents/HBaseTest.xls";
		// 取所有工作簿表名
		// List<String> sheets = ExcelPOIHelper.GetSheets(fullPath);
		String sheetName = "Sheet1";
		
		// 取所有的行
		Map<String, List<Map<String, String>>> rows = new HashMap<String, List<Map<String,String>>>();
		rows = ExcelPOIHelper.GetRows(fullPath, sheetName);
		
		Iterator rowItertor = rows.entrySet().iterator();
		while (rowItertor.hasNext()) {
			Entry rowEntry = (Entry)rowItertor.next();
			Object rowKey = rowEntry.getKey();
			Object cellsValue = rowEntry.getValue();
			
			List<Map<String, String>> cells = new ArrayList<Map<String,String>>();
			cells = (List<Map<String,String>>) cellsValue;
			Iterator<Map<String, String>> it = cells.iterator();
			while (it.hasNext()) {
				Object cellObject = it.next();
				
				Map<String, String> cell = new HashMap<String, String>();
				cell = (HashMap<String, String>) cellObject;
				
			Iterator cellIterator = cell.entrySet().iterator();
			while (cellIterator.hasNext()) {
				Entry cellEntry = (Entry)cellIterator.next();
				Object cellColumn = cellEntry.getKey();
				Object value = cellEntry.getValue();
				
				String strCellColumn = cellColumn.toString();
				String colunmName = strCellColumn.split("<")[0];
			}
			}
		}
	}*/
}
