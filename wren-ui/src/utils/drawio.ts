export type DrawIoColumn = {
  name: string;
  type: string;
  isPrimaryKey: boolean;
};

export type DrawIoTable = {
  name: string;
  columns: DrawIoColumn[];
};

export type DrawIoRelationType = 'ONE_TO_ONE' | 'ONE_TO_MANY' | 'MANY_TO_ONE';

export type DrawIoRelation = {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  type: DrawIoRelationType;
};

export type DrawIoImportPayload = {
  tables: DrawIoTable[];
  relations: DrawIoRelation[];
};

type DrawIoField = {
  id: string;
  tableId: string;
  tableName: string;
  name: string;
  type?: string;
  isPrimaryKey: boolean;
  isRelationLabel: boolean;
};

const PRIMARY_KEY_MARKER = '\u{1F511}';
const RELATION_MARKER = '\u{1F517}';
const RELATION_ARROW = '\u2192';
const DEFAULT_TYPE = 'UNKNOWN';

const stripTags = (value: string) => value.replace(/<[^>]+>/g, '');

const parseTableName = (value: string) =>
  stripTags(value).replace(/\s*\([^)]*\)\s*$/, '').trim();

const parseRelationType = (
  value?: string | null,
  style?: string | null,
): DrawIoRelationType => {
  const normalized = (value || '').replace(/\s/g, '').toUpperCase();
  if (normalized === '1:1') return 'ONE_TO_ONE';
  if (normalized === '1:N' || normalized === '1:M') return 'ONE_TO_MANY';
  if (normalized === 'N:1' || normalized === 'M:1') return 'MANY_TO_ONE';

  const styleText = style || '';
  const startArrow = /startArrow=([^;]+)/.exec(styleText)?.[1] || '';
  const endArrow = /endArrow=([^;]+)/.exec(styleText)?.[1] || '';
  const start = startArrow.toLowerCase();
  const end = endArrow.toLowerCase();

  if (start.includes('many') && end.includes('one')) return 'MANY_TO_ONE';
  if (start.includes('one') && end.includes('many')) return 'ONE_TO_MANY';
  if (start.includes('one') && end.includes('one')) return 'ONE_TO_ONE';

  return 'MANY_TO_ONE';
};

const parseFieldValue = (value: string) => {
  const cleaned = stripTags(value).trim();
  if (!cleaned) return null;

  const isPrimaryKey =
    cleaned.includes(PRIMARY_KEY_MARKER) || /\(PK\)/i.test(cleaned);
  const isRelationLabel =
    cleaned.includes(RELATION_MARKER) || cleaned.includes(RELATION_ARROW);

  let text = cleaned
    .replace(PRIMARY_KEY_MARKER, '')
    .replace(RELATION_MARKER, '')
    .replace(/\(PK\)/i, '')
    .trim();

  if (text.includes(RELATION_ARROW)) {
    text = text.split(RELATION_ARROW)[0].trim();
  }

  const colonIndex = text.indexOf(':');
  const name = (colonIndex === -1 ? text : text.slice(0, colonIndex)).trim();
  const type =
    colonIndex === -1 ? undefined : text.slice(colonIndex + 1).trim();

  if (!name) return null;

  return {
    name,
    type,
    isPrimaryKey,
    isRelationLabel,
  };
};

const mergeColumn = (
  columns: Map<string, DrawIoColumn>,
  field: DrawIoField,
) => {
  const existing = columns.get(field.name);
  const nextType = field.type && field.type.length ? field.type : DEFAULT_TYPE;
  if (!existing) {
    columns.set(field.name, {
      name: field.name,
      type: nextType,
      isPrimaryKey: field.isPrimaryKey,
    });
    return;
  }

  if (field.isPrimaryKey && !existing.isPrimaryKey) {
    existing.isPrimaryKey = true;
  }

  if (existing.type === DEFAULT_TYPE && nextType !== DEFAULT_TYPE) {
    existing.type = nextType;
  }
};

export const parseDrawIoXml = (xml: string): DrawIoImportPayload => {
  if (!xml || !xml.trim()) {
    throw new Error('Draw.io XML is empty.');
  }

  if (typeof DOMParser === 'undefined') {
    throw new Error('XML parsing is not available in this environment.');
  }

  const parser = new DOMParser();
  const doc = parser.parseFromString(xml, 'text/xml');
  if (doc.getElementsByTagName('parsererror').length > 0) {
    throw new Error('Invalid XML. Please check your draw.io export.');
  }

  const cells = Array.from(doc.getElementsByTagName('mxCell'));
  const tablesById = new Map<string, { id: string; name: string }>();
  const fields: DrawIoField[] = [];

  for (const cell of cells) {
    const id = cell.getAttribute('id') || '';
    const vertex = cell.getAttribute('vertex');
    if (vertex !== '1' || !id.startsWith('table_')) continue;

    const value = cell.getAttribute('value') || '';
    const name = parseTableName(value);
    if (!name) continue;

    tablesById.set(id, { id, name });
  }

  for (const cell of cells) {
    const id = cell.getAttribute('id') || '';
    const vertex = cell.getAttribute('vertex');
    if (vertex !== '1' || !id.startsWith('field_')) continue;

    const parentId = cell.getAttribute('parent') || '';
    const table = tablesById.get(parentId);
    if (!table) continue;

    const value = cell.getAttribute('value') || '';
    const parsed = parseFieldValue(value);
    if (!parsed) continue;

    fields.push({
      id,
      tableId: parentId,
      tableName: table.name,
      ...parsed,
    });
  }

  const columnsByTable = new Map<string, Map<string, DrawIoColumn>>();
  const fieldToColumnName = new Map<string, string>();
  const fieldToTableName = new Map<string, string>();

  for (const field of fields) {
    fieldToTableName.set(field.id, field.tableName);
  }

  for (const table of tablesById.values()) {
    const tableFields = fields.filter((field) => field.tableId === table.id);
    const columns = new Map<string, DrawIoColumn>();

    for (const field of tableFields) {
      if (field.isRelationLabel) continue;
      mergeColumn(columns, field);
    }

    for (const field of tableFields) {
      if (!field.isRelationLabel) continue;
      if (columns.has(field.name)) {
        fieldToColumnName.set(field.id, field.name);
        continue;
      }
      mergeColumn(columns, field);
    }

    for (const field of tableFields) {
      if (!fieldToColumnName.has(field.id)) {
        fieldToColumnName.set(field.id, field.name);
      }
    }

    columnsByTable.set(table.name, columns);
  }

  const relations: DrawIoRelation[] = [];
  const seenRelations = new Set<string>();

  for (const cell of cells) {
    const isEdge = cell.getAttribute('edge') === '1';
    if (!isEdge) continue;

    const sourceId = cell.getAttribute('source') || '';
    const targetId = cell.getAttribute('target') || '';
    if (!sourceId || !targetId) continue;

    const fromTable = fieldToTableName.get(sourceId);
    const toTable = fieldToTableName.get(targetId);
    const fromColumn = fieldToColumnName.get(sourceId);
    const toColumn = fieldToColumnName.get(targetId);

    if (!fromTable || !toTable || !fromColumn || !toColumn) continue;

    const relationType = parseRelationType(
      cell.getAttribute('value'),
      cell.getAttribute('style'),
    );

    const key = `${fromTable}.${fromColumn}->${toTable}.${toColumn}:${relationType}`;
    if (seenRelations.has(key)) continue;

    seenRelations.add(key);
    relations.push({
      fromTable,
      fromColumn,
      toTable,
      toColumn,
      type: relationType,
    });
  }

  const tables: DrawIoTable[] = Array.from(columnsByTable.entries()).map(
    ([tableName, columns]) => ({
      name: tableName,
      columns: Array.from(columns.values()),
    }),
  );

  return { tables, relations };
};
