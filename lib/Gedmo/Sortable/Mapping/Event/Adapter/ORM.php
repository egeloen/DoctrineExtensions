<?php

namespace Gedmo\Sortable\Mapping\Event\Adapter;

use Doctrine\Common\Persistence\Mapping\ClassMetadata;
use Doctrine\Common\Persistence\Mapping\ClassMetadataFactory;
use Doctrine\Common\Util\ClassUtils;
use Doctrine\ORM\QueryBuilder;
use Gedmo\Mapping\Event\Adapter\ORM as BaseAdapterORM;
use Gedmo\Sortable\Mapping\Event\SortableAdapter;

/**
 * Doctrine event adapter for ORM adapted
 * for sortable behavior
 *
 * @author Lukas Botsch <lukas.botsch@gmail.com>
 * @license MIT License (http://www.opensource.org/licenses/mit-license.php)
 */
final class ORM extends BaseAdapterORM implements SortableAdapter
{
    public function getMaxPosition(array $config, $meta, $groups)
    {
        $em = $this->getObjectManager();
        $metaFactory = $em->getMetadataFactory();

        $qb = $em->createQueryBuilder();
        $qb->select('MAX(n.' . $config['position'] . ')')
            ->from($config['useObjectClass'], 'n');
        $this->addGroupWhere($qb, $groups, $meta, $metaFactory);
        $query = $qb->getQuery();
        $query->useQueryCache(false);
        $query->useResultCache(false);
        $res = $query->getResult();

        return $res[0][1];
    }

    private function addGroupWhere(QueryBuilder $qb, $groups, $meta, $metaFactory)
    {
        $i = 1;
        $j = 1;

        foreach ($groups as $group => $value) {
            $groupFields = explode('.', $group);
            $field = array_pop($groupFields);
            $alias = 'n';

            foreach ($groupFields as $groupField) {
                $qb->innerJoin($alias.'.' . $groupField, $alias = 'n__' . $j);
                $j++;
            }

            if (null === $value) {
                $qb->andWhere($qb->expr()->isNull($alias.'.' . $field));
            } else {
                $qb->andWhere($alias . '.' . $field . ' = :group__' . $i);
                $qb->setParameter('group__' . $i, $this->getGroupValue($value), $this->getGroupType($group, $value, $meta, $metaFactory));
            }
            $i++;
        }
    }

    /**
     * @param $value
     * @return bool
     */
    private function isEntity($value)
    {
        return (is_object($value) && $this->getObjectManager()->getMetadataFactory()->hasMetadataFor(ClassUtils::getClass($value)));
    }

    /**
     * @param $value
     * @return mixed
     */
    private function getGroupValue($value)
    {
        if (!$this->isEntity($value)) {
            return $value;
        }
        return $this->getObjectManager()->getUnitOfWork()->getSingleIdentifierValue($value);
    }

    /**
     * @param string               $group
     * @param mixed                $value
     * @param ClassMetadata        $meta
     * @param ClassMetadataFactory $metaFactory
     *
     * @return \Doctrine\DBAL\Types\Type|null|string
     */
    private function getGroupType($group, $value, $meta, $metaFactory)
    {
        if (!$this->isEntity($value)) {
            if (!$meta instanceof ClassMetadata) {
                return null;
            }

            $groupFields = explode('.', $group);
            $field = array_pop($groupFields);

            foreach ($groupFields as $groupField) {
                $meta = $metaFactory->getMetadataFor($groupField);
            }

            return $meta->getTypeOfField($field);
        }

        $metaData = $this->getObjectManager()->getClassMetadata(ClassUtils::getClass($value));
        $ids = $metaData->getIdentifier();
        if (count($ids) > 1) {
            return null;
        }
        return $metaData->getTypeOfField($ids[0]);
    }

    public function updatePositions($relocation, $delta, $config)
    {
        $om = $this->getObjectManager();
        $metaFactory = $om->getMetadataFactory();

        $sign = $delta['delta'] < 0 ? "-" : "+";
        $absDelta = abs($delta['delta']);

        $updateDql = "UPDATE {$relocation['name']} r";
        $updateDql .= " SET r.{$config['position']} = r.{$config['position']} {$sign} {$absDelta}";
        $updateDql .= " WHERE r.id IN (:ids)";

        $selectDql = "SELECT n.id FROM {$relocation['name']} n";
        $selectDqlJoin = "";
        $selectDqlWhere = " WHERE n.{$config['position']} >= {$delta['start']}";

        // if not null, false or 0
        if ($delta['stop'] > 0) {
            $selectDqlWhere .= " AND n.{$config['position']} < {$delta['stop']}";
        }

        $i = -1;
        $j = -1;
        $params = array();
        foreach ($relocation['groups'] as $group => $value) {
            $groupFields = explode('.', $group);
            $field = array_pop($groupFields);
            $alias = 'n';

            foreach ($groupFields as $groupField) {
                $selectDqlJoin .= " INNER JOIN {$alias}.{$groupField} " . ($alias = "n__" . (++$j));
            }

            if (null === $value) {
                $selectDqlWhere .= " AND {$alias}.{$field} IS NULL";
            } else {
                $selectDqlWhere .= " AND {$alias}.{$field} = :val___" . (++$i);
                $params['val___' . $i] = $value;
            }
        }

        $meta = $om->getClassMetadata($relocation['name']);
        // add excludes
        if (!empty($delta['exclude'])) {
            $meta = $om->getClassMetadata($relocation['name']);
            if (count($meta->identifier) == 1) {
                // if we only have one identifier, we can use IN syntax, for better performance
                $excludedIds = array();

                foreach ($delta['exclude'] as $entity) {
                    if ($id = $meta->getFieldValue($entity, $meta->identifier[0])) {
                        $excludedIds[] = $id;
                    }
                }

                if (!empty($excludedIds)) {
                    $params['excluded'] = $excludedIds;
                    $selectDqlWhere .= " AND n.{$meta->identifier[0]} NOT IN (:excluded)";
                }
            } else {
                if (count($meta->identifier) > 1) {
                    foreach ($delta['exclude'] as $entity) {
                        $j = 0;
                        $selectDqlWhere .= " AND NOT (";

                        foreach ($meta->getIdentifierValues($entity) as $id => $value) {
                            $selectDqlWhere .= ($j > 0 ? " AND " : "") . "n.{$id} = :val___" . (++$i);
                            $params['val___' . $i] = $value;
                            $j++;
                        }

                        $selectDqlWhere .= ")";
                    }
                }
            }
        }

        $selectDql .= $selectDqlJoin . $selectDqlWhere;

        $em = $this->getObjectManager();
        $q = $em->createQuery($selectDql);
        $q->setParameters($params);

        foreach ($relocation['groups'] as $group => $value) {
            if (!is_null($value)) {
                $q->setParameter('val___' . $i, $this->getGroupValue($value), $this->getGroupType($group, $value, $meta, $metaFactory));
            }
        }

        $ids = $q->getScalarResult();

        if (!empty($ids)) {
            $q = $em->createQuery($updateDql);
            $q->setParameter('ids', $ids);
            $q->execute();
        }
    }
}
