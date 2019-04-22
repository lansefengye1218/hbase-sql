package org.dxer.hbase.sql.visitor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.RegexStringComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.dxer.hbase.HBaseSqlContants;
import org.dxer.hbase.sql.util.ExpressionUtil;
import org.dxer.hbase.util.HBaseSqlMapParser;

import com.google.common.base.Strings;

import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.DateValue;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.ExpressionVisitor;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.JdbcParameter;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.NullValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.TimeValue;
import net.sf.jsqlparser.expression.TimestampValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseAnd;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseOr;
import net.sf.jsqlparser.expression.operators.arithmetic.BitwiseXor;
import net.sf.jsqlparser.expression.operators.arithmetic.Concat;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.Matches;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItemVisitor;
import net.sf.jsqlparser.statement.select.Limit;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SelectVisitor;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.WithItem;

/**
 * Created by linghf on 2016/12/21.
 */
public class SelectSqlVisitor implements SelectVisitor, FromItemVisitor, ExpressionVisitor {

	private List<String> tableNames = new ArrayList<String>();

	private Set<String> queryColumns = new HashSet<String>();

	private Map<String, List<String>> queryColumnMap = new HashMap<String, List<String>>();

	private FilterList filterList = new FilterList();

	private String startRow;

	private String stopRow;

	private Scan scanner = new Scan();

	private Long rowCount;

	private Long offset;

	public SelectSqlVisitor(Select select) {
		SelectBody selectBody = select.getSelectBody();
		selectBody.accept(this);
	}

	public String getTableName() {
		return tableNames.get(0);
	}

	public Set<String> getQueryColumns() {
		return queryColumns;
	}

	public Map<String, List<String>> getQueryColumnMap() {
		return queryColumnMap;
	}

	public Long getOffset() {
		return offset;
	}

	public Long getRowCount() {
		return rowCount;
	}

	public Scan getScanner() {
		if (filterList != null && filterList.getFilters() != null && filterList.getFilters().size() > 0) {
			scanner.setFilter(filterList);
		}
		if (!Strings.isNullOrEmpty(startRow)) {
			scanner.setStartRow(Bytes.toBytes(startRow));
		}
		if (!Strings.isNullOrEmpty(stopRow)) {
			scanner.setStopRow(Bytes.toBytes(stopRow));
		}
		scanner.setCaching(1000);

		return scanner;
	}

	public void visit(NullValue nullValue) {

	}

	public void visit(Function function) {

	}

	/*
	 * public void visit(SignedExpression signedExpression) {
	 * 
	 * }
	 */

	public void visit(JdbcParameter jdbcParameter) {

	}

	/*
	 * public void visit(JdbcNamedParameter jdbcNamedParameter) {
	 * 
	 * }
	 */

	public void visit(DoubleValue doubleValue) {

	}

	public void visit(LongValue longValue) {

	}

	/*
	 * public void visit(HexValue hexValue) {
	 * 
	 * }
	 */

	public void visit(DateValue dateValue) {

	}

	public void visit(TimeValue timeValue) {

	}

	public void visit(TimestampValue timestampValue) {

	}

	public void visit(Parenthesis parenthesis) {

	}

	public void visit(StringValue stringValue) {

	}

	public void visit(Addition addition) {

	}

	public void visit(Division division) {

	}

	public void visit(Multiplication multiplication) {

	}

	public void visit(Subtraction subtraction) {

	}

	public void visit(AndExpression andExpression) {
		andExpression.getLeftExpression().accept(this);
		andExpression.getRightExpression().accept(this);
	}

	public void visit(OrExpression orExpression) {
		orExpression.getLeftExpression().accept(this);
		orExpression.getRightExpression().accept(this);
	}

	public void visit(Between between) {
		String x = between.getLeftExpression().toString();
		String start = between.getBetweenExpressionStart().toString();
		String end = between.getBetweenExpressionEnd().toString();

		System.out.println(x + " between " + start + " and " + end);
	}

	public void visit(EqualsTo equalsTo) {
		String key = equalsTo.getLeftExpression().toString();
		String value = equalsTo.getRightExpression().toString();

		if (!Strings.isNullOrEmpty(key) && !Strings.isNullOrEmpty(value)) {
			if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(key)) {
				RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(value));
				filterList.addFilter(rowFilter);
			} else if (HBaseSqlContants.START_ROW.equalsIgnoreCase(key)) {
				this.startRow = value;
			} else if (HBaseSqlContants.STOP_ROW.equalsIgnoreCase(key)) {
				this.stopRow = value;
			}
		}
	}

	public void visit(GreaterThan greaterThan) {

	}

	public void visit(GreaterThanEquals greaterThanEquals) {

	}

	public void visit(InExpression inExpression) {
		String key = inExpression.getLeftExpression().toString();

		ItemsList itemsList = inExpression.getRightItemsList();
		List<String> values = ExpressionUtil.getStringList(itemsList);

		List<Filter> filters = new ArrayList<Filter>();
		if (values != null && !values.isEmpty()) {
			for (String value : values) {
				if (Strings.isNullOrEmpty(value)) {
					continue;
				}
				if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(key)) {
					RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
							new RegexStringComparator(value));
					filters.add(rowFilter);
				}
			}
		}
		if (filters != null && !filters.isEmpty()) {
			FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);
			filterList.addFilter(list);
		}
	}

	public void visit(IsNullExpression isNullExpression) {

	}

	public void visit(LikeExpression likeExpression) {
		String left = likeExpression.getLeftExpression().toString();
		String right = likeExpression.getRightExpression().toString();

		if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(left)) {
			Filter rkFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(right));
			filterList.addFilter(rkFilter);
		} else {
			String tableName = getTableName();
			org.dxer.hbase.entity.Table table = HBaseSqlMapParser.getTable(tableName);
			if (table != null && table.getRowKey() != null) {
				if (table.getRowKey().getAliasName().equals(left)) {
					Filter rkFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(right));
					filterList.addFilter(rkFilter);
				}
			}
		}
	}

	public void visit(MinorThan minorThan) {

	}

	public void visit(MinorThanEquals minorThanEquals) {

	}

	public void visit(NotEqualsTo notEqualsTo) {

	}

	public void visit(Column column) {

	}

	public void visit(CaseExpression caseExpression) {

	}

	public void visit(WhenClause whenClause) {

	}

	public void visit(ExistsExpression existsExpression) {

	}

	public void visit(AllComparisonExpression allComparisonExpression) {

	}

	public void visit(AnyComparisonExpression anyComparisonExpression) {

	}

	public void visit(Concat concat) {

	}

	public void visit(Matches matches) {

	}

	public void visit(BitwiseAnd bitwiseAnd) {

	}

	public void visit(BitwiseOr bitwiseOr) {

	}

	public void visit(BitwiseXor bitwiseXor) {

	}

	public void visit(CastExpression castExpression) {

	}

	public void visit(Modulo modulo) {

	}

	public void visit(AnalyticExpression analyticExpression) {

	}

	public void visit(WithinGroupExpression withinGroupExpression) {

	}

	public void visit(ExtractExpression extractExpression) {

	}

	public void visit(IntervalExpression intervalExpression) {

	}

	public void visit(OracleHierarchicalExpression oracleHierarchicalExpression) {

	}

	public void visit(RegExpMatchOperator regExpMatchOperator) {

	}

	public void visit(JsonExpression jsonExpression) {

	}

	public void visit(RegExpMySQLOperator regExpMySQLOperator) {

	}

	public void visit(UserVariable userVariable) {

	}

	public void visit(NumericBind numericBind) {

	}

	public void visit(KeepExpression keepExpression) {

	}

	public void visit(MySQLGroupConcat mySQLGroupConcat) {

	}

	public void visit(RowConstructor rowConstructor) {

	}

	public void visit(OracleHint oracleHint) {

	}

	public void visit(TimeKeyExpression timeKeyExpression) {

	}

	public void visit(DateTimeLiteralExpression dateTimeLiteralExpression) {

	}

	public void visit(Table table) {
		if (table != null && !Strings.isNullOrEmpty(table.getName())) {
			tableNames.add(table.getName());
		}
	}

	public void visit(SubSelect subSelect) {
		System.out.println(subSelect.getWithItemsList());
	}

	public void visit(SubJoin subJoin) {

	}

	public void visit(LateralSubSelect lateralSubSelect) {

	}

	public void visit(ValuesList valuesList) {

	}

	public void visit(TableFunction tableFunction) {

	}

	/**
	 * 设置column
	 *
	 * @param selectItems
	 */
	private void setQueryColumns(List<SelectItem> selectItems) {
		if (selectItems == null || selectItems.size() <= 0) {
			return;
		}
		for (SelectItem item : selectItems) {
			String colStr = item.toString();
			if (Strings.isNullOrEmpty(colStr)) {
				continue;
			}

			String[] columnGroup = colStr.split(".");
			if (columnGroup != null && columnGroup.length == 2) {
				String columnFamily = columnGroup[0];
				String column = columnGroup[1];
				if (Strings.isNullOrEmpty(columnFamily) || Strings.isNullOrEmpty(column)) {
					List<String> columns = queryColumnMap.get(columnGroup[0]);
					if (columns == null) {
						columns = new ArrayList<String>();
					}
					columns.add(column);
					queryColumnMap.put(columnFamily, columns);

					scanner.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(column));
				}
			} else if (HBaseSqlContants.ROW_KEY.equalsIgnoreCase(colStr)) {

			} else if (HBaseSqlContants.ASTERISK.equalsIgnoreCase(colStr)) {

			}
			queryColumns.add(colStr);
		}
	}

	public void visit(PlainSelect plainSelect) {
		if (plainSelect == null) {
			return;
		}
		List<SelectItem> selectItems = plainSelect.getSelectItems();
		setQueryColumns(selectItems);

		Limit limit = plainSelect.getLimit();

		if (limit != null) {
			offset = limit.getOffset();
			rowCount = limit.getRowCount();
		}

		plainSelect.getFromItem().accept(this);
		if (plainSelect.getWhere() != null) {
			plainSelect.getWhere().accept(this);
		}
	}

	public void visit(SetOperationList setOperationList) {

	}

	public void visit(WithItem withItem) {

	}

	public void visit(InverseExpression inverseExpression) {
		// TODO Auto-generated method stub

	}

	public void visit(Union union) {
		// TODO Auto-generated method stub

	}

}
