// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuildstmt

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/docs"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemaexpr"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scerrors"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// CreateIndex implements CREATE INDEX.
func CreateIndex(b BuildCtx, n *tree.CreateIndex) {
	// Resolve the table name and start building the new index element.
	relationElements := b.ResolveRelation(n.Table.ToUnresolvedObjectName(), ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   privilege.CREATE,
	})
	// We don't support handling zone config related properties for tables, so
	// throw an unsupported error.
	if _, _, tbl := scpb.FindTable(relationElements); tbl != nil {
		fallBackIfZoneConfigExists(b, n, tbl.TableID)
	} else if _, _, view := scpb.FindView(relationElements); view != nil {
	} else {
		panic(errors.AssertionFailedf("unable to find table/view element for %s", n.Table.String()))
	}
	if len(n.StorageParams) > 0 {
		panic(scerrors.NotImplementedErrorf(n,
			"FIXME: Storage parameters.."))
	}
	if n.Predicate != nil {
		panic(scerrors.NotImplementedErrorf(n,
			"FIXME: partial index support"))
	}
	_, _, partitionAllBy := scpb.FindTablePartitionAllBy(relationElements)
	if partitionAllBy != nil && n.PartitionByIndex != nil &&
		n.PartitionByIndex.ContainsPartitions() {
		panic(pgerror.New(
			pgcode.FeatureNotSupported,
			"cannot define PARTITION BY on an index if the table has a PARTITION ALL BY definition",
		))
	}

	// Inverted indexes do not support hash sharing or unique.
	if n.Inverted {
		if n.Sharded != nil {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support hash sharding"))
		}
		if len(n.Storing) > 0 {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes don't support stored columns"))
		}
		if n.Unique {
			panic(pgerror.New(pgcode.InvalidSQLStatementName, "inverted indexes can't be unique"))
		}
	}
	var indexSpec indexSpec
	indexSpec.secondary = &scpb.SecondaryIndex{
		Index: scpb.Index{
			IsUnique:       n.Unique,
			IsInverted:     n.Inverted,
			IsConcurrently: n.Concurrently,
			IsNotVisible:   n.NotVisible,
		},
	}
	var relation scpb.Element
	relationElements.ForEachElementStatus(func(_ scpb.Status, target scpb.TargetStatus, e scpb.Element) {
		switch t := e.(type) {
		case *scpb.Table:
			n.Table.ObjectNamePrefix = b.NamePrefix(t)
			if descpb.IsVirtualTable(t.TableID) {
				return
			}
			indexSpec.secondary.TableID = t.TableID
			indexSpec.secondary.IndexID = b.NextTableIndexID(t)
			relation = e

		case *scpb.View:
			n.Table.ObjectNamePrefix = b.NamePrefix(t)
			if !t.IsMaterialized {
				return
			}
			if n.Sharded != nil {
				panic(pgerror.New(pgcode.InvalidObjectDefinition,
					"cannot create hash sharded index on materialized view"))
			}
			indexSpec.secondary.TableID = t.ViewID
			indexSpec.secondary.IndexID = b.NextViewIndexID(t)
			relation = e

		case *scpb.TableLocalityGlobal, *scpb.TableLocalityPrimaryRegion, *scpb.TableLocalitySecondaryRegion:
			if n.PartitionByIndex != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"cannot define PARTITION BY on a new INDEX in a multi-region database",
				))
			}

		case *scpb.TableLocalityRegionalByRow:
			if n.PartitionByIndex != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported,
					"cannot define PARTITION BY on a new INDEX in a multi-region database",
				))
			}
			if n.Sharded != nil {
				panic(pgerror.New(pgcode.FeatureNotSupported, "hash sharded indexes are not compatible with REGIONAL BY ROW tables"))
			}

		case *scpb.PrimaryIndex:
			// TODO(ajwerner): This is too simplistic. We should build a better
			// vocabulary around the possible primary indexes in play. There are
			// at most going to be 3, and at least there is going to be 1. If
			// there are no column set changes, or there's just additions of
			// nullable columns there'll be just one. If there are only either
			// adds or drops, but not both, there will be two, the initial and
			// the final. If there are both adds and drops, then there will be
			// 3, including an intermediate primary index which is keyed on the
			// initial primary key and include the union of all of the added and
			// dropped columns.
			if target == scpb.ToPublic {
				indexSpec.primary = t
			}
		}
	})
	if n.Unique {
		indexSpec.secondary.ConstraintID = b.NextTableConstraintID(indexSpec.secondary.TableID)
	}
	if indexSpec.secondary.TableID == catid.InvalidDescID || indexSpec.primary == nil {
		panic(pgerror.Newf(pgcode.WrongObjectType,
			"%q is not a table or materialized view", n.Table.ObjectName))
	}
	// Assign the IDs for the index.
	tempIndexID := indexSpec.secondary.IndexID + 1
	indexSpec.secondary.SourceIndexID = indexSpec.primary.IndexID
	indexSpec.secondary.TemporaryIndexID = tempIndexID
	// Resolve the index name and make sure it doesn't exist yet.
	{
		indexElements := b.ResolveIndex(indexSpec.secondary.TableID, n.Name, ResolveParams{
			IsExistenceOptional: true,
			RequiredPrivilege:   privilege.CREATE,
		})
		if _, target, sec := scpb.FindSecondaryIndex(indexElements); sec != nil {
			if n.IfNotExists {
				return
			}
			if target == scpb.ToAbsent {
				panic(pgerror.Newf(pgcode.ObjectNotInPrerequisiteState,
					"index %q being dropped, try again later", n.Name.String()))
			}
			panic(pgerror.Newf(pgcode.DuplicateRelation, "index with name %q already exists", n.Name))
		}
	}
	addColumnsForSecondaryIndex(b, n, relation, &indexSpec)
	maybeAddPartitionDescriptorForIndex(b, n, &indexSpec)
	// Warn against creating a non-partitioned index on a partitioned table,
	// which is undesirable in most cases.
	// Avoid the warning if we have PARTITION ALL BY as all indexes will implicitly
	// have relevant partitioning columns prepended at the front.
	scpb.ForEachIndexPartitioning(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
		if target == scpb.ToPublic &&
			e.IndexID == indexSpec.primary.IndexID && e.TableID == indexSpec.primary.TableID &&
			e.NumColumns > 0 &&
			partitionAllBy == nil {
			b.EvalCtx().ClientNoticeSender.BufferClientNotice(
				b,
				errors.WithHint(
					pgnotice.Newf("creating non-partitioned index on partitioned table may not be performant"),
					"Consider modifying the index such that it is also partitioned.",
				),
			)
		}
	})
	// Recompute the ordinal in kind value for keys, since
	// we just added implicit columns.
	keyIdx := 0
	keySuffixIdx := 0
	for _, ic := range indexSpec.columns {
		if ic.Kind == scpb.IndexColumn_KEY {
			ic.OrdinalInKind = uint32(keyIdx)
			keyIdx++
		} else if ic.Kind == scpb.IndexColumn_KEY_SUFFIX {
			ic.OrdinalInKind = uint32(keySuffixIdx)
			keySuffixIdx++
		}
	}
	indexSpec.data = &scpb.IndexData{TableID: indexSpec.secondary.TableID, IndexID: indexSpec.secondary.IndexID}
	indexSpec.temporary = &scpb.TemporaryIndex{
		Index:                    protoutil.Clone(indexSpec.secondary).(*scpb.SecondaryIndex).Index,
		IsUsingSecondaryEncoding: true,
	}

	indexSpec.temporary.TemporaryIndexID = 0
	indexSpec.temporary.IndexID = tempIndexID

	// Construct the temporary objects from the index spec, since these will
	// be transient.
	b.AddTransient(&scpb.IndexData{TableID: indexSpec.temporary.TableID, IndexID: indexSpec.temporary.IndexID})
	b.AddTransient(indexSpec.temporary)

	for _, ic := range indexSpec.columns {
		tic := protoutil.Clone(ic).(*scpb.IndexColumn)
		tic.IndexID = indexSpec.temporary.IndexID
		b.Add(tic)
	}
	if indexSpec.partitioning != nil {
		partitionDesc := protoutil.Clone(&indexSpec.partitioning.PartitioningDescriptor).(*catpb.PartitioningDescriptor)
		b.AddTransient(&scpb.IndexPartitioning{
			TableID:                indexSpec.temporary.TableID,
			IndexID:                indexSpec.temporary.IndexID,
			PartitioningDescriptor: *partitionDesc,
		})
	}
	indexSpec.temporary = nil
	indexSpec.primary = nil
	indexSpec.apply(b.Add)
	// Apply the name once everything else has been created, since we need
	// elements to resolve things.
	indexName := string(n.Name)
	if indexName == "" {
		numImplicitColumns := 0
		_, _, tbl := scpb.FindTable(relationElements)
		scpb.ForEachIndexPartitioning(b, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID == indexSpec.secondary.IndexID && e.TableID == indexSpec.secondary.TableID {
				numImplicitColumns = int(e.PartitioningDescriptor.NumImplicitColumns)
			}
		})
		indexName = getImplicitSecondaryIndexName(b, tbl, indexSpec.secondary.IndexID, numImplicitColumns)
	}
	b.Add(&scpb.IndexName{
		TableID: indexSpec.secondary.TableID,
		IndexID: indexSpec.secondary.IndexID,
		Name:    indexName,
	})
	if n.Concurrently {
		b.EvalCtx().ClientNoticeSender.BufferClientNotice(b,
			pgnotice.Newf("CONCURRENTLY is not required as all indexes are created concurrently"),
		)
	}
}

func addColumnsForSecondaryIndex(
	b BuildCtx, n *tree.CreateIndex, relation scpb.Element, indexSpec *indexSpec,
) {
	tableID := screl.GetDescID(relation)
	relationElements := b.QueryByID(tableID)

	// Check that the index creation spec is sane.
	columnRefs := map[string]struct{}{}
	columnExprRefs := map[string]struct{}{}
	for _, columnNode := range n.Columns {
		if columnNode.Expr == nil {
			colName := columnNode.Column.Normalize()
			if _, found := columnRefs[colName]; found {
				panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
					"index %q contains duplicate column %q", n.Name, colName))
			}
			columnRefs[colName] = struct{}{}
		} else {
			colExpr := columnNode.Expr.String()
			if _, found := columnExprRefs[colExpr]; found {
				panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
					"index %q contains duplicate expression", n.Name))
			}
			columnExprRefs[colExpr] = struct{}{}
		}
	}
	for _, storingNode := range n.Storing {
		colName := storingNode.Normalize()
		if _, found := columnRefs[colName]; found {
			panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
				"index %q already contains column %q", n.Name, colName))
		}
		columnRefs[colName] = struct{}{}
	}
	// Set key column IDs and directions.
	keyColNames := make([]string, len(n.Columns))
	var keyColIDs catalog.TableColSet
	indexID := indexSpec.secondary.IndexID
	lastColumnIdx := len(n.Columns) - 1
	for i, columnNode := range n.Columns {
		colName := columnNode.Column.String()
		normalizedColName := columnNode.Column.Normalize()
		// Disallow descending last columns in inverted indexes.
		if n.Inverted && columnNode.Direction == tree.Descending {
			panic(pgerror.New(pgcode.FeatureNotSupported,
				"the last column in an inverted index cannot have the DESC option"))
		}
		if columnNode.Expr != nil {
			tbl, ok := relation.(*scpb.Table)
			if !ok {
				panic(scerrors.NotImplementedErrorf(n,
					"indexing virtual column expressions in materialized views is not supported"))
			}
			colName = maybeCreateVirtualColumnForIndex(b, &n.Table, tbl, columnNode.Expr, n.Inverted, i == len(n.Columns)-1)
			normalizedColName = colName
		}
		var columnID catid.ColumnID
		colElts := b.ResolveColumn(tableID, tree.Name(normalizedColName), ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)
		columnID = column.ColumnID
		_, _, columnType := scpb.FindColumnType(colElts)
		processColNodeType(b, n, colName, columnNode, columnType, i == lastColumnIdx)
		// Column should be accessible.
		if columnNode.Expr == nil {
			checkColumnAccessibilityForIndex(colName, colElts, false)
		}
		keyColNames[i] = colName
		direction := catpb.IndexColumn_ASC
		if columnNode.Direction == tree.Descending {
			direction = catpb.IndexColumn_DESC
		}
		ic := &scpb.IndexColumn{
			TableID:       tableID,
			IndexID:       indexID,
			ColumnID:      columnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY,
			Direction:     direction,
		}
		indexSpec.columns = append(indexSpec.columns, ic)
		keyColIDs.Add(columnID)
	}
	// Set the key suffix column IDs.
	// We want to find the key column IDs
	var keySuffixColumns []*scpb.IndexColumn
	scpb.ForEachIndexColumn(relationElements, func(
		current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn,
	) {
		if e.IndexID != indexSpec.primary.IndexID || keyColIDs.Contains(e.ColumnID) ||
			e.Kind != scpb.IndexColumn_KEY {
			return
		}
		// Check if the column name was duplicated from the STORING clause, in which
		// case this isn't allowed.
		// Note: The column IDs for the key columns are not resolved, so we couldn't
		// do this earlier.
		scpb.ForEachColumnName(relationElements, func(current scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
			if cn.ColumnID == e.ColumnID &&
				cn.TableID == e.TableID {
				if _, found := columnRefs[cn.Name]; found {
					panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
						"index %q already contains column %q", n.Name, cn.Name))
				}
				columnRefs[cn.Name] = struct{}{}
			}
		})
		keySuffixColumns = append(keySuffixColumns, e)
	})
	sort.Slice(keySuffixColumns, func(i, j int) bool {
		return keySuffixColumns[i].OrdinalInKind < keySuffixColumns[j].OrdinalInKind
	})
	for i, c := range keySuffixColumns {
		ic := &scpb.IndexColumn{
			TableID:       tableID,
			IndexID:       indexID,
			ColumnID:      c.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_KEY_SUFFIX,
			Direction:     c.Direction,
		}
		indexSpec.columns = append(indexSpec.columns, ic)
	}

	// Set the storing column IDs.
	for i, storingNode := range n.Storing {
		colElts := b.ResolveColumn(tableID, storingNode, ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)
		checkColumnAccessibilityForIndex(storingNode.String(), colElts, true)
		c := &scpb.IndexColumn{
			TableID:       tableID,
			IndexID:       indexID,
			ColumnID:      column.ColumnID,
			OrdinalInKind: uint32(i),
			Kind:          scpb.IndexColumn_STORED,
		}
		indexSpec.columns = append(indexSpec.columns, c)
	}
	// Set up sharding.
	if n.Sharded != nil {
		buckets, err := tabledesc.EvalShardBucketCount(b, b.SemaCtx(), b.EvalCtx(), n.Sharded.ShardBuckets, n.StorageParams)
		if err != nil {
			panic(err)
		}
		shardColName := maybeCreateAndAddShardCol(b, int(buckets), relation.(*scpb.Table), keyColNames, n)
		indexSpec.secondary.Sharding = &catpb.ShardedDescriptor{
			IsSharded:    true,
			Name:         shardColName,
			ShardBuckets: buckets,
			ColumnNames:  keyColNames,
		}
		colElts := b.ResolveColumn(tableID, tree.Name(shardColName), ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, column := scpb.FindColumn(colElts)

		indexColumn := &scpb.IndexColumn{
			IndexID:       indexID,
			TableID:       column.TableID,
			ColumnID:      column.ColumnID,
			OrdinalInKind: 0,
			Kind:          scpb.IndexColumn_KEY,
			Direction:     catpb.IndexColumn_ASC,
		}
		indexSpec.columns = append([]*scpb.IndexColumn{indexColumn}, indexSpec.columns...)
	}
}

func maybeAddPartitionDescriptorForIndex(b BuildCtx, n *tree.CreateIndex, indexSpec *indexSpec) {
	tableID := indexSpec.primary.TableID
	relationElts := b.QueryByID(tableID)
	var indexPartitioningDesc *scpb.IndexPartitioning
	// If partition by all is required, then setup the information
	// for that.
	_, _, partitionAllBy := scpb.FindTablePartitionAllBy(relationElts)
	if partitionAllBy != nil {
		// Update AST to reflect the partition information.
		n.PartitionByIndex = &tree.PartitionByIndex{
			PartitionBy: &tree.PartitionBy{},
		}
		var indexImplicitCols []*scpb.IndexColumn
		scpb.ForEachIndexColumn(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexColumn) {
			if e.IndexID == indexSpec.primary.IndexID && e.Implicit {
				indexImplicitCols = append(indexImplicitCols, e)
			}
		})
		sort.Slice(indexImplicitCols, func(i, j int) bool {
			return indexImplicitCols[i].OrdinalInKind < indexImplicitCols[j].OrdinalInKind
		})

		for _, ic := range indexImplicitCols {
			scpb.ForEachColumnName(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnName) {
				if e.ColumnID == ic.ColumnID && e.TableID == ic.TableID {
					n.PartitionByIndex.Fields = append(n.PartitionByIndex.Fields, tree.Name(e.Name))
				}
			})
		}
		// Recover the rest from the partitioning data.
		scpb.ForEachIndexPartitioning(relationElts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.IndexPartitioning) {
			if e.IndexID == indexSpec.primary.IndexID {
				indexPartitioningDesc = protoutil.Clone(e).(*scpb.IndexPartitioning)
			}
		})
	}

	if n.PartitionByIndex.ContainsPartitions() || indexPartitioningDesc != nil {
		if indexPartitioningDesc == nil {
			indexPartitioningDesc = &scpb.IndexPartitioning{
				TableID: indexSpec.secondary.TableID,
				IndexID: indexSpec.secondary.IndexID,
				PartitioningDescriptor: b.IndexPartitioningDescriptor(n.Name.String(),
					&indexSpec.secondary.Index, indexSpec.columns, n.PartitionByIndex.PartitionBy),
			}
		} else {
			indexPartitioningDesc.IndexID = indexSpec.secondary.IndexID
		}
		indexSpec.partitioning = indexPartitioningDesc
		var columnsToPrepend []*scpb.IndexColumn
		for _, field := range n.PartitionByIndex.Fields {
			// Resolve the column first.
			ers := b.ResolveColumn(tableID, field, ResolveParams{RequiredPrivilege: privilege.CREATE})
			_, _, fieldColumn := scpb.FindColumn(ers)
			// If it already in the index we are done.
			// FIXME: Deal with sharding
			if fieldColumn.ColumnID == indexSpec.columns[0].ColumnID {
				break
			}
			newIndexColumn := &scpb.IndexColumn{
				IndexID:   indexSpec.secondary.IndexID,
				TableID:   fieldColumn.TableID,
				ColumnID:  fieldColumn.ColumnID,
				Kind:      scpb.IndexColumn_KEY,
				Direction: catpb.IndexColumn_ASC,
				Implicit:  true,
			}
			// Check if the column is already a suffix, then we should
			// move it out.
			for pos, otherIC := range indexSpec.columns {
				if otherIC.ColumnID == fieldColumn.ColumnID {
					indexSpec.columns = append(indexSpec.columns[:pos], indexSpec.columns[pos+1:]...)
				}
			}

			columnsToPrepend = append(columnsToPrepend, newIndexColumn)
		}
		indexSpec.columns = append(columnsToPrepend, indexSpec.columns...)
	}
}

func newUndefinedOpclassError(opclass tree.Name) error {
	return pgerror.Newf(pgcode.UndefinedObject, "operator class %q does not exist", opclass)
}

func checkColumnAccessibilityForIndex(colName string, columnElts ElementResultSet, store bool) {
	_, _, column := scpb.FindColumn(columnElts)

	if column.IsInaccessible {
		panic(pgerror.Newf(
			pgcode.UndefinedColumn,
			"column %q is inaccessible and cannot be referenced",
			colName))
	}
	if column.IsSystemColumn {
		if store {
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"index cannot store system column %s",
				colName))
		} else {
			panic(pgerror.Newf(
				pgcode.FeatureNotSupported,
				"cannot index system column %s",
				colName))
		}
	}
}

func processColNodeType(
	b BuildCtx,
	n *tree.CreateIndex,
	colName string,
	columnNode tree.IndexElem,
	columnType *scpb.ColumnType,
	lastColIdx bool,
) {
	if columnType.Type.Family() == types.GeometryFamily ||
		columnType.Type.Family() == types.GeographyFamily {
		panic(scerrors.NotImplementedErrorf(n,
			"FIXME: Storage parameters.."))
	}
	// OpClass are only allowed for the last column of an inverted index.
	if columnNode.OpClass != "" && (!lastColIdx || !n.Inverted) {
		panic(pgerror.New(pgcode.DatatypeMismatch,
			"operator classes are only allowed for the last column of an inverted index"))
	}
	if n.Inverted && columnNode.OpClass != "" {
		switch columnType.Type.Family() {
		case types.ArrayFamily:
			switch columnNode.OpClass {
			case "array_ops", "":
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
		case types.JsonFamily:
			switch columnNode.OpClass {
			case "jsonb_ops", "":
			case "jsonb_path_ops":
				panic(unimplemented.NewWithIssue(81115, "operator class \"jsonb_path_ops\" is not supported"))
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
		case types.GeometryFamily:
			panic("unexpected")
		case types.GeographyFamily:
			panic("unexpected")

		case types.StringFamily:
			// Check the opclass of the last column in the list, which is the column
			// we're going to inverted index.
			switch columnNode.OpClass {
			default:
				panic(newUndefinedOpclassError(columnNode.OpClass))
			}
		}
	}
	// Only certain column types are supported for inverted indexes.
	if n.Inverted && lastColIdx &&
		!colinfo.ColumnTypeIsInvertedIndexable(columnType.Type) {
		colNameForErr := colName
		if columnNode.Expr != nil {
			colNameForErr = columnNode.Expr.String()
		}
		panic(tabledesc.NewInvalidInvertedColumnError(colNameForErr,
			columnType.Type.String()))
	} else if (!n.Inverted || !lastColIdx) &&
		!colinfo.ColumnTypeIsIndexable(columnType.Type) {
		// Otherwise, check if the column type is indexable.
		panic(pgerror.Newf(
			pgcode.InvalidTableDefinition,
			"index element %s of type %s is not indexable",
			colName,
			columnType.Type))
	}
}
func nextRelationIndexID(b BuildCtx, relation scpb.Element) catid.IndexID {
	switch t := relation.(type) {
	case *scpb.Table:
		return b.NextTableIndexID(t)
	case *scpb.View:
		return b.NextViewIndexID(t)
	default:
		panic(errors.AssertionFailedf("unexpected relation element of type %T", relation))
	}
}

// maybeCreateAndAddShardCol adds a new hidden computed shard column (or its mutation) to
// `desc`, if one doesn't already exist for the given index column set and number of shard
// buckets.
func maybeCreateAndAddShardCol(
	b BuildCtx, shardBuckets int, tbl *scpb.Table, colNames []string, n tree.NodeFormatter,
) (shardColName string) {
	shardColName = tabledesc.GetShardColumnName(colNames, int32(shardBuckets))
	elts := b.QueryByID(tbl.TableID)
	// TODO(ajwerner): In what ways is the column referenced by
	//  existingShardColID allowed to differ from the newly made shard column?
	//  Should there be some validation of the existing shard column?
	var existingShardColID catid.ColumnID
	scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, name *scpb.ColumnName) {
		if target == scpb.ToPublic && name.Name == shardColName {
			existingShardColID = name.ColumnID
		}
	})
	scpb.ForEachColumn(elts, func(_ scpb.Status, _ scpb.TargetStatus, col *scpb.Column) {
		if col.ColumnID == existingShardColID && !col.IsHidden {
			// The user managed to reverse-engineer our crazy shard column name, so
			// we'll return an error here rather than try to be tricky.
			panic(pgerror.Newf(pgcode.DuplicateColumn,
				"column %s already specified; can't be used for sharding", shardColName))
		}
	})
	if existingShardColID != 0 {
		return shardColName
	}
	expr := schemaexpr.MakeHashShardComputeExpr(colNames, shardBuckets)
	parsedExpr, err := parser.ParseExpr(*expr)
	if err != nil {
		panic(err)
	}
	shardColID := b.NextTableColumnID(tbl)
	spec := addColumnSpec{
		tbl: tbl,
		col: &scpb.Column{
			TableID:        tbl.TableID,
			ColumnID:       shardColID,
			IsHidden:       true,
			PgAttributeNum: catid.PGAttributeNum(shardColID),
		},
		name: &scpb.ColumnName{
			TableID:  tbl.TableID,
			ColumnID: shardColID,
			Name:     shardColName,
		},
		colType: &scpb.ColumnType{
			TableID:     tbl.TableID,
			ColumnID:    shardColID,
			TypeT:       scpb.TypeT{Type: types.Int},
			ComputeExpr: b.WrapExpression(tbl.TableID, parsedExpr),
			IsVirtual:   true,
		},
	}
	addColumn(b, spec, n)
	return shardColName
}

func maybeCreateVirtualColumnForIndex(
	b BuildCtx, tn *tree.TableName, tbl *scpb.Table, expr tree.Expr, inverted bool, lastColumn bool,
) string {
	validateColumnIndexableType := func(t *types.T) {
		if t.IsAmbiguous() {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"type of index element %s is ambiguous",
					expr.String(),
				),
				"consider adding a type cast to the expression",
			))
		}
		// Check if the column type is indexable,
		// non-inverted types.
		if !inverted &&
			!colinfo.ColumnTypeIsIndexable(t) {
			panic(pgerror.Newf(
				pgcode.InvalidTableDefinition,
				"index element %s of type %s is not indexable",
				expr,
				t.Name()))
		}
		// Check if inverted columns are invertible.
		if inverted &&
			!lastColumn &&
			!colinfo.ColumnTypeIsIndexable(t) {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"index element %s of type %s is not allowed as a prefix column in an inverted index",
					expr.String(),
					t.Name(),
				),
				"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
			))
		}
		if inverted &&
			lastColumn &&
			!colinfo.ColumnTypeIsInvertedIndexable(t) {
			panic(errors.WithHint(
				pgerror.Newf(
					pgcode.InvalidTableDefinition,
					"index element %s of type %s is not allowed as the last column in an inverted index",
					expr,
					t.Name(),
				),
				"see the documentation for more information about inverted indexes: "+docs.URL("inverted-indexes.html"),
			))
		}
	}
	elts := b.QueryByID(tbl.TableID)
	colName := ""
	// Check if any existing columns can satisfy this expression already.
	scpb.ForEachColumnType(elts, func(current scpb.Status, target scpb.TargetStatus, e *scpb.ColumnType) {
		if target == scpb.ToPublic && e.ComputeExpr != nil {
			otherExpr, err := parser.ParseExpr(string(e.ComputeExpr.Expr))
			if err != nil {
				panic(err)
			}
			if otherExpr.String() == expr.String() {
				validateColumnIndexableType(e.Type)
				scpb.ForEachColumnName(elts, func(current scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
					if target == scpb.ToPublic && e.ColumnID == cn.ColumnID && e.TableID == cn.TableID {
						colName = cn.Name
					}
				})
			}
		}
	})
	if colName != "" {
		return colName
	}
	// Otherwise, we need to create a new column.
	colName = tabledesc.GenerateUniqueName("crdb_internal_idx_expr", func(name string) (found bool) {
		scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
			if target == scpb.ToPublic && cn.Name == name {
				found = true
			}
		})
		return found
	})
	// TODO(postamar): call addColumn instead of building AST.
	d := &tree.ColumnTableDef{
		Name: tree.Name(colName),
	}
	d.Computed.Computed = true
	d.Computed.Virtual = true
	d.Computed.Expr = expr
	d.Nullable.Nullability = tree.Null
	// Infer column type from expression.
	{
		colLookupFn := func(columnName tree.Name) (exists bool, accessible bool, id catid.ColumnID, typ *types.T) {
			scpb.ForEachColumnName(elts, func(_ scpb.Status, target scpb.TargetStatus, cn *scpb.ColumnName) {
				if target == scpb.ToPublic && tree.Name(cn.Name) == columnName {
					id = cn.ColumnID
				}
			})
			if id == 0 {
				return false, false, 0, nil
			}
			scpb.ForEachColumn(elts, func(_ scpb.Status, target scpb.TargetStatus, col *scpb.Column) {
				if target == scpb.ToPublic && col.ColumnID == id {
					exists = true
					accessible = !col.IsInaccessible
				}
			})
			scpb.ForEachColumnType(elts, func(_ scpb.Status, target scpb.TargetStatus, col *scpb.ColumnType) {
				if target == scpb.ToPublic && col.ColumnID == id {
					typ = col.Type
				}
			})
			return exists, accessible, id, typ
		}
		replacedExpr, _, err := schemaexpr.ReplaceColumnVars(expr, colLookupFn)
		if err != nil {
			panic(err)
		}
		typedExpr, err := tree.TypeCheck(b, replacedExpr, b.SemaCtx(), types.Any)
		if err != nil {
			panic(err)
		}
		d.Type = typedExpr.ResolvedType()
		validateColumnIndexableType(typedExpr.ResolvedType())
	}
	alterTableAddColumn(b, tn, tbl, &tree.AlterTableAddColumn{ColumnDef: d})
	// Mutate the accessibility flag on this column it should be inaccessible.
	{
		ers := b.ResolveColumn(tbl.TableID, d.Name, ResolveParams{
			RequiredPrivilege: privilege.CREATE,
		})
		_, _, col := scpb.FindColumn(ers)
		col.IsInaccessible = true
	}
	return colName
}
