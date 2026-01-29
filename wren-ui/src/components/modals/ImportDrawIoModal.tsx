import { useState } from 'react';
import { Button, Input, Modal, Space, Typography, message } from 'antd';
import { parseDrawIoXml } from '@/utils/drawio';

const { Text } = Typography;
const { TextArea } = Input;

type Props = {
  visible: boolean;
  onClose: () => void;
  onImported?: () => Promise<void> | void;
};

export default function ImportDrawIoModal(props: Props) {
  const { visible, onClose, onImported } = props;
  const [xml, setXml] = useState('');
  const [loading, setLoading] = useState(false);

  const handleImport = async () => {
    if (!xml.trim()) {
      message.warning('Please paste your draw.io XML first.');
      return;
    }

    try {
      setLoading(true);
      const payload = parseDrawIoXml(xml);
      if (!payload.tables.length) {
        throw new Error('No tables found in the XML.');
      }

      const response = await fetch('/api/v1/import_drawio', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });

      const result = await response.json();
      if (!response.ok) {
        throw new Error(result?.message || 'Import failed.');
      }

      message.success(
        `Imported ${result.models} models, ${result.columns} columns, ${result.relations} relations.`,
      );

      if (result.skippedRelations) {
        message.warning(`Skipped ${result.skippedRelations} relations.`);
      }

      setXml('');
      onClose();
      await onImported?.();
    } catch (error: any) {
      message.error(error?.message || 'Import failed.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <Modal
      visible={visible}
      title="Import draw.io XML"
      onCancel={onClose}
      footer={null}
      width={720}
      destroyOnClose
    >
      <Space direction="vertical" size={12} style={{ width: '100%' }}>
        <Text type="secondary">
          Paste your draw.io XML. This will replace the current models and
          relations.
        </Text>
        <TextArea
          value={xml}
          onChange={(event) => setXml(event.target.value)}
          placeholder="Paste draw.io XML here"
          autoSize={{ minRows: 10, maxRows: 18 }}
        />
        <Space style={{ justifyContent: 'flex-end', width: '100%' }}>
          <Button onClick={onClose} disabled={loading}>
            Cancel
          </Button>
          <Button type="primary" onClick={handleImport} loading={loading}>
            Import
          </Button>
        </Space>
      </Space>
    </Modal>
  );
}
