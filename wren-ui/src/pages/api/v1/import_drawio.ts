import { NextApiRequest, NextApiResponse } from 'next';
import { components } from '@/common';
import type { RelationType } from '@server/types';
import { transformInvalidColumnName, replaceInvalidReferenceName } from '@server/utils';
import { getLogger } from '@server/utils';

const logger = getLogger('API_IMPORT_DRAWIO');
logger.level = 'debug';

const {
  projectService,
  modelRepository,
  modelColumnRepository,
  relationRepository,
} = components;

type DrawIoColumn = {
  name: string;
  type?: string;
  isPrimaryKey?: boolean;
};

type DrawIoTable = {
  name: string;
  columns: DrawIoColumn[];
};

type DrawIoRelation = {
  fromTable: string;
  fromColumn: string;
  toTable: string;
  toColumn: string;
  type: RelationType;
};

type DrawIoImportPayload = {
  tables: DrawIoTable[];
  relations: DrawIoRelation[];
};

const capitalize = (value: string) =>
  value.length ? value.charAt(0).toUpperCase() + value.slice(1) : value;

export const config = {
  api: {
    bodyParser: {
      sizeLimit: '4mb',
    },
  },
};

export default async function handler(
  req: NextApiRequest,
  res: NextApiResponse,
) {
  if (req.method !== 'POST') {
    res.status(405).json({ message: 'Method not allowed' });
    return;
  }

  try {
    const payload = req.body as DrawIoImportPayload;
    if (!payload?.tables || payload.tables.length === 0) {
      res.status(400).json({ message: 'No tables provided.' });
      return;
    }

    const project = await projectService.getCurrentProject();
    const tableNameSet = new Set<string>();
    const modelReferenceSet = new Set<string>();
    const emptyProperties = JSON.stringify({});

    for (const table of payload.tables) {
      if (!table?.name) {
        res.status(400).json({ message: 'Table name is required.' });
        return;
      }
      if (tableNameSet.has(table.name)) {
        res
          .status(400)
          .json({ message: `Duplicate table name: ${table.name}` });
        return;
      }
      tableNameSet.add(table.name);
    }

    const tx = await modelRepository.transaction();

    try {
      await relationRepository.deleteAllBy({ projectId: project.id }, { tx });
      await modelRepository.deleteAllBy({ projectId: project.id }, { tx });

      const modelValues = payload.tables.map((table) => {
        const baseReferenceName = replaceInvalidReferenceName(table.name);
        let referenceName = baseReferenceName;
        let suffix = 1;
        while (modelReferenceSet.has(referenceName)) {
          suffix += 1;
          referenceName = `${baseReferenceName}_${suffix}`;
        }
        modelReferenceSet.add(referenceName);

        const modelProperties: Record<string, string> = {
          table: table.name,
        };
        if (project.schema) {
          modelProperties.schema = project.schema;
        }
        if (project.catalog) {
          modelProperties.catalog = project.catalog;
        }

        return {
          projectId: project.id,
          displayName: table.name,
          sourceTableName: table.name,
          referenceName,
          cached: false,
          refreshTime: null,
          properties: JSON.stringify(modelProperties),
        };
      });

      const models = await modelRepository.createMany(modelValues, { tx });
      const modelByName = new Map(
        models.map((model) => [model.sourceTableName, model]),
      );

      const columnValues = [] as any[];
      const columnKeySet = new Set<string>();
      const columnRefNameByModel = new Map<number, Set<string>>();

      for (const table of payload.tables) {
        const model = modelByName.get(table.name);
        if (!model) continue;

        for (const column of table.columns || []) {
          if (!column?.name) continue;
          const key = `${model.id}::${column.name}`;
          if (columnKeySet.has(key)) continue;
          columnKeySet.add(key);

          let referenceName = transformInvalidColumnName(column.name);
          const usedRefNames =
            columnRefNameByModel.get(model.id) || new Set<string>();
          if (usedRefNames.has(referenceName)) {
            let suffix = 1;
            let candidate = `${referenceName}_${suffix}`;
            while (usedRefNames.has(candidate)) {
              suffix += 1;
              candidate = `${referenceName}_${suffix}`;
            }
            referenceName = candidate;
          }
          usedRefNames.add(referenceName);
          columnRefNameByModel.set(model.id, usedRefNames);

          columnValues.push({
            modelId: model.id,
            isCalculated: false,
            displayName: column.name,
            referenceName,
            sourceColumnName: column.name,
            type: column.type || 'UNKNOWN',
            notNull: false,
            isPk: Boolean(column.isPrimaryKey),
            properties: emptyProperties,
          });
        }
      }

      const columns = columnValues.length
        ? await modelColumnRepository.createMany(columnValues, { tx })
        : [];

      const columnByKey = new Map<string, any>();
      for (const column of columns) {
        columnByKey.set(`${column.modelId}::${column.sourceColumnName}`, column);
      }

      const relationValues = [] as any[];
      const relationNameSet = new Set<string>();
      let skippedRelations = 0;

      for (const relation of payload.relations || []) {
        const fromModel = modelByName.get(relation.fromTable);
        const toModel = modelByName.get(relation.toTable);
        if (!fromModel || !toModel) {
          skippedRelations += 1;
          continue;
        }

        const fromColumn = columnByKey.get(
          `${fromModel.id}::${relation.fromColumn}`,
        );
        const toColumn = columnByKey.get(
          `${toModel.id}::${relation.toColumn}`,
        );

        if (!fromColumn || !toColumn) {
          skippedRelations += 1;
          continue;
        }

        const baseName =
          capitalize(fromModel.sourceTableName) +
          capitalize(fromColumn.referenceName) +
          capitalize(toModel.sourceTableName) +
          capitalize(toColumn.referenceName);
        let name = baseName;
        let suffix = 1;
        while (relationNameSet.has(name)) {
          suffix += 1;
          name = `${baseName}_${suffix}`;
        }
        relationNameSet.add(name);

        relationValues.push({
          projectId: project.id,
          name,
          fromColumnId: fromColumn.id,
          toColumnId: toColumn.id,
          joinType: relation.type,
          properties: null,
        });
      }

      const relations = relationValues.length
        ? await relationRepository.createMany(relationValues, { tx })
        : [];

      await tx.commit();

      res.status(200).json({
        models: models.length,
        columns: columns.length,
        relations: relations.length,
        skippedRelations,
      });
    } catch (error: any) {
      await tx.rollback();
      throw error;
    }
  } catch (error: any) {
    logger.error('Failed to import draw.io XML', error);
    res.status(500).json({ message: error?.message || 'Import failed.' });
  }
}
