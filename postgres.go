package repository

// Based on https://github.com/AJRDRGZ/db-query-builder

import (
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
)

const _defaultMaxLimit = 100

// BuildSQLInsert builds a query INSERT of postgres
func BuildSQLInsert(table string, fields []string) string {
	var args, vals string

	for k, v := range fields {
		args += fmt.Sprintf("%s,", v)
		vals += fmt.Sprintf("$%d,", k+2)
	}

	if len(fields) > 0 {
		args = args[:len(args)-1]
		vals = vals[:len(vals)-1]
	}

	return fmt.Sprintf("INSERT INTO %s (id,%s) VALUES ($1,%s) RETURNING created_at", table, args, vals)
}

func BuildSQLInsertNoID(table string, fields []string) string {
	var args, vals string

	for k, v := range fields {
		args += fmt.Sprintf("%s,", v)
		vals += fmt.Sprintf("$%d,", k+1)
	}

	if len(fields) > 0 {
		args = args[:len(args)-1]
		vals = vals[:len(vals)-1]
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) RETURNING id, created_at", table, args, vals)
}

// BuildSQLUpdateByID builds a query UPDATE of postgres
func BuildSQLUpdateByID(table string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var args string
	for k, v := range fields {
		args += fmt.Sprintf("%s = $%d, ", v, k+1)
	}

	return fmt.Sprintf("UPDATE %s SET %supdated_at = now() WHERE id = $%d", table, args, len(fields)+1)
}

// BuildSQLUpdateBy builds a query UPDATE of postgres
func BuildSQLUpdateBy(table, byField string, fields Fields) (string, []any) {
	var args []any
	if fields.IsEmpty() {
		return "", args
	}

	var fieldToUpdate string
	for k, field := range fields {
		fieldToUpdate += fmt.Sprintf("%s = $%d, ", field.Name, k+1)

		if field.Value != nil {
			args = append(args, field.Value)
		}
	}

	return fmt.Sprintf("UPDATE %s SET %supdated_at = now() WHERE %s = $%d", table, fieldToUpdate, byField, len(fields)+1), args
}

// BuildSQLSelectFields builds a query SELECT of postgres
func BuildSQLSelectFields(table string, fields []string) string {
	if len(fields) == 0 {
		return ""
	}

	var args string
	for _, v := range fields {
		args += fmt.Sprintf("%s, ", v)
	}

	return fmt.Sprintf("SELECT %s FROM %s", args[:len(args)-2], table)
}

func BuildSQLWhereWithSequence(fields Fields, paramSequence int) (string, []interface{}) {
	if fields.IsEmpty() {
		return "", []interface{}{}
	}

	query := "WHERE"
	length := len(fields)
	lastFieldIndex := length - 1
	nGroups := 0
	var args []interface{}

	for key, field := range fields {
		setChainingField(&field)
		setOperatorField(&field)
		setAliases(&field)
		setGroupOpen(&field)

		if field.GroupOpen {
			nGroups++
		}

		switch field.Operator {
		case In, NotIn:
			query = fmt.Sprintf("%s %s", query, BuildINNotIN(field, field.Operator))
		case IsNull, IsNotNull:
			query = fmt.Sprintf("%s %s %s",
				query,
				strings.ToLower(field.Name),
				field.Operator,
			)
		case Between:
			// TODO: improve this function to return an error instead of string
			if err := field.ValidateFromAndToValues(); err != nil {
				return err.Error(), nil
			}

			query = fmt.Sprintf("%s %s %s $%d AND $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
				paramSequence+1,
			)

			// Increment paramSequence because `BETWEEN` has 2 params always
			paramSequence++
		case Parenthesis:
			query += strings.ToLower(field.Name)
		default:
			// if we need to compare against the column of other table
			if field.IsValueFromTable {
				query = fmt.Sprintf("%s %s %s %s",
					query,
					strings.ToLower(field.Name),
					field.Operator,
					strings.ToLower(field.NameValueFromTable),
				)

				break
			}

			// if we compare against a value that we define
			query = fmt.Sprintf("%s %s %s $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
			)
		}

		// Close the group
		if (nGroups > 0) && field.GroupClose {
			nGroups--
			query += ")"
		}

		// if exists still groups open, close them in the last field
		if (nGroups > 0) && (key == lastFieldIndex) {
			query += strings.Repeat(")", nGroups)
		}

		// Add chainingKey (OR, AND) except in the last field
		if key != lastFieldIndex {
			query = fmt.Sprintf("%s %s", query, field.ChainingKey)
		}

		if field.Operator == In ||
			field.Operator == IsNull ||
			field.Operator == IsNotNull ||
			field.IsValueFromTable {

			continue
		}

		// Add arguments of the parameters when operator is different to "IN, IsNull, IsNotNull" or when IsValueFromTable is true
		if field.Value != nil {
			args = append(args, field.Value)
		}
		if field.Operator == Between {
			args = append(args, field.FromValue, field.ToValue)
		}

		paramSequence++
	}

	return query, args
}

// BuildSQLWhere builds and returns a query not WHERE of postgres and its arguments
func BuildSQLWhere(fields Fields) (string, []any) {
	if fields.IsEmpty() {
		return "", []any{}
	}

	query := "WHERE"
	length := len(fields)
	lastFieldIndex := length - 1
	nGroups := 0
	var args []interface{}

	paramSequence := 1
	for key, field := range fields {
		setChainingField(&field)
		setOperatorField(&field)
		setAliases(&field)
		setGroupOpen(&field)

		if field.GroupOpen {
			nGroups++
		}

		switch field.Operator {
		case In, NotIn:
			query = fmt.Sprintf("%s %s", query, BuildINNotIN(field, field.Operator))
		case IsNull, IsNotNull:
			query = fmt.Sprintf("%s %s %s",
				query,
				strings.ToLower(field.Name),
				field.Operator,
			)
		case Between:
			if err := field.ValidateFromAndToValues(); err != nil {
				return err.Error(), nil
			}

			query = fmt.Sprintf("%s %s %s $%d AND $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
				paramSequence+1,
			)

			// Increment paramSequence because `BETWEEN` has 2 params always
			paramSequence++
		case Parenthesis:
			query += strings.ToLower(field.Name)
		default:
			// if we need to compare against the column of other table
			if field.IsValueFromTable {
				query = fmt.Sprintf("%s %s %s %s",
					query,
					strings.ToLower(field.Name),
					field.Operator,
					strings.ToLower(field.NameValueFromTable),
				)

				break
			}

			// if we compare against a value that we define
			query = fmt.Sprintf("%s %s %s $%d",
				query,
				strings.ToLower(field.Name),
				field.Operator,
				paramSequence,
			)
		}

		// Close the group
		if (nGroups > 0) && field.GroupClose {
			nGroups--
			query += ")"
		}

		// if exists still groups open, close them in the last field
		if (nGroups > 0) && (key == lastFieldIndex) {
			query += strings.Repeat(")", nGroups)
		}

		// Add chainingKey (OR, AND) except in the last field
		if key != lastFieldIndex && field.Operator != Parenthesis {
			query = fmt.Sprintf("%s %s", query, field.ChainingKey)
		}

		if field.Operator == In ||
			field.Operator == IsNull ||
			field.Operator == IsNotNull ||
			field.IsValueFromTable {

			continue
		}

		// Add arguments of the parameters when operator is different to "IN, IsNull, IsNotNull" or when IsValueFromTable is true
		if field.Value != nil {
			args = append(args, field.Value)
		}
		if field.Operator == Between {
			args = append(args, field.FromValue, field.ToValue)
		}

		if field.Operator == Parenthesis {
			continue
		}

		paramSequence++
	}

	return query, args
}

// BuildSQLOrderBy builds and returns a query ORDER BY of postgres and its arguments
func BuildSQLOrderBy(sorts SortFields) string {
	if sorts.IsEmpty() {
		return ""
	}

	query, length := "ORDER BY", len(sorts)

	for key, sort := range sorts {
		setSortFieldOrder(&sort)
		setSortFieldAliases(&sort)
		query = fmt.Sprintf("%s %s %s",
			query,
			strings.ToLower(sort.Name),
			sort.Order,
		)
		if key != (length - 1) {
			query = fmt.Sprintf("%s,", query)
		}
	}

	return query
}

// BuildSQLPagination builds and returns a query OFFSET LIMIT of postgres for pagination
func BuildSQLPagination(pag Pagination) string {
	if pag.Limit == 0 && pag.Page == 0 {
		return ""
	}
	if pag.MaxLimit == 0 {
		pag.MaxLimit = _defaultMaxLimit
	}

	if pag.Limit == 0 || pag.Limit > pag.MaxLimit {
		pag.Limit = pag.MaxLimit
	}
	if pag.Page == 0 {
		pag.Page = 1
	}

	offset := pag.Page*pag.Limit - pag.Limit

	pagination := fmt.Sprintf("LIMIT %d OFFSET %d", pag.Limit, offset)

	return pagination
}

// BuildQueryAndArgs builds and returns a query adding the filter + sort
func BuildQueryAndArgs(initialSQL string, specification FieldsSpecification) (string, []interface{}) {
	conditions, args := BuildSQLWhere(specification.Filters)
	query := initialSQL + " " + conditions

	query += " " + BuildSQLOrderBy(specification.Sorts)
	query += " " + BuildSQLPagination(specification.Pagination)

	return query, args
}

// BuildQueryArgsAndPagination builds and returns a query adding the filter + sort + pagination
func BuildQueryArgsAndPagination(initialSQL string, specification FieldsSpecification) (string, []interface{}) {
	conditions, args := BuildSQLWhere(specification.Filters)
	query := initialSQL + " " + conditions

	query += " " + BuildSQLOrderBy(specification.Sorts)
	query += " " + BuildSQLPagination(specification.Pagination)

	return query, args
}

// BuildQueryArgsAndPaginationWithSequence builds and returns a query adding the filter + sort + pagination
func BuildQueryArgsAndPaginationWithSequence(initialSQL string, specification FieldsSpecification, initSequenceFilters int) (string, []interface{}) {
	conditions, args := BuildSQLWhereWithSequence(specification.Filters, initSequenceFilters)
	query := initialSQL + " " + conditions

	query += " " + BuildSQLOrderBy(specification.Sorts)
	query += " " + BuildSQLPagination(specification.Pagination)

	return query, args
}

// ColumnsAliased return the column names with aliased of the table
func ColumnsAliased(fields []string, aliased string) string {
	if len(fields) == 0 {
		return ""
	}
	columns := ""
	for _, v := range fields {
		columns += fmt.Sprintf("%s.%s, ", aliased, v)
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns, aliased, aliased)
}

// ColumnsAliasedWithDefault return the column names with aliased of the table
func ColumnsAliasedWithDefault(fields []string, aliased string) string {
	if len(fields) == 0 {
		return ""
	}
	columns := ""
	for _, v := range fields {
		columns += fmt.Sprintf("%s.%s, ", aliased, v)
	}

	return fmt.Sprintf("%s.id, %s%s.created_at, %s.updated_at",
		aliased, columns, aliased, aliased)
}

func BuildINNotIN(field Field, operator operatorField) string {
	nameField := strings.ToLower(field.Name)
	// if the IN failed, return mistakeIN for not select nothing in the field
	mistakeIN := fmt.Sprintf("%s = ''", nameField)

	var args string
	switch items := field.Value.(type) {
	case []uint:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("%d,", item)
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	case []int:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("%d,", item)
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	case []int64:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("%d,", item)
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	case []string:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("'%s',", item)
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	case []uuid.UUID:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			args += fmt.Sprintf("'%s',", item)
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	case []time.Time:
		if len(items) == 0 {
			return mistakeIN
		}

		for _, item := range items {
			year, month, day := item.Date()
			args += fmt.Sprintf("'%s',", fmt.Sprintf("%d-%d-%d", year, month, day))
		}

		return fmt.Sprintf("%s %s (%s)", nameField, operator, strings.TrimSuffix(args, ","))
	default:
		return mistakeIN
	}
}

func setChainingField(field *Field) {
	if field.Operator == Parenthesis {
		field.ChainingKey = ""
		return
	}

	if field.ChainingKey == "" {
		field.ChainingKey = And
	}
}

func setOperatorField(field *Field) {
	if field.Operator == "" {
		field.Operator = Equals
	}
}

func setAliases(field *Field) {
	if field.Source != "" {
		field.Name = fmt.Sprintf("%s.%s", field.Source, field.Name)
	}

	if field.SourceNameValueFromTable != "" {
		field.NameValueFromTable = fmt.Sprintf("%s.%s", field.SourceNameValueFromTable, field.NameValueFromTable)
	}
}

func setGroupOpen(field *Field) {
	if field.Operator == Parenthesis && field.GroupOpen {
		field.Name = " ("
		return
	}

	if field.GroupOpen {
		field.Name = fmt.Sprintf("(%s", field.Name)
	}
}

func setSortFieldOrder(sortField *SortField) {
	if sortField.Order == "" {
		sortField.Order = Asc
	}
}

func setSortFieldAliases(sortField *SortField) {
	if sortField.Source != "" {
		sortField.Name = fmt.Sprintf("%s.%s", sortField.Source, sortField.Name)
	}
}
