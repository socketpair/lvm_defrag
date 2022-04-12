#!/usr/bin/python3
import json
import logging
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from operator import attrgetter
from subprocess import check_call


@dataclass
class Pvseg:
    vg_name: str
    lv_name: str
    pvseg_start: int
    seg_start_pe: int
    seg_size_pe: int
    segtype: str


log = logging.getLogger(__name__)


def defrag(pv_name: str) -> bool:
    # Wait and report any movements
    log.info('Waiting for move operation to complete.')
    check_call(['pvmove'])
    log.info('Complete. Inspecting state.')

    pvoffset2seg: dict[int, Pvseg] = dict()
    vg_lv_name2segments: dict[str, list[Pvseg]] = defaultdict(list)

    for pvseg_ in json.loads(
            subprocess.check_output([
                'pvs',
                '--segments',
                '-ovg_name,lv_name,pvseg_start,seg_start_pe,seg_size_pe,segtype',
                '--reportformat=json',
                pv_name,
            ])
    )['report'][0]['pvseg']:
        lvm_seg = Pvseg(
            vg_name=pvseg_['vg_name'],
            lv_name=pvseg_['lv_name'],
            pvseg_start=int(pvseg_['pvseg_start']),
            seg_start_pe=int(pvseg_['seg_start_pe']),
            seg_size_pe=int(pvseg_['seg_size_pe']),
            segtype=pvseg_['segtype'],
        )

        pvoffset2seg[lvm_seg.pvseg_start] = lvm_seg

        if lvm_seg.segtype == 'free':
            pass
        elif lvm_seg.segtype == 'linear':
            vg_lv_name2segments[lvm_seg.vg_name + '/' + lvm_seg.lv_name].append(lvm_seg)
        else:
            raise RuntimeError(f'Unknown segment type {lvm_seg.segtype!r}.')

    # Order by LOGICAL start (may be non-contiguous) # TODO: warn about it.
    for lvm_segs in vg_lv_name2segments.values():
        lvm_segs.sort(key=attrgetter('seg_start_pe'))

    expected_pvseg_offset = 0

    # Отсортируем lvm по смещению физического сегмента их первого логического сегмента.
    # Логические сегменты отсортированы ранее.
    for vg_lv_name, lvm_segs in sorted(vg_lv_name2segments.items(), key=lambda record: record[1][0].pvseg_start):
        log.info('Inspecting %s.', vg_lv_name)
        for lvm_seg in lvm_segs:
            if lvm_seg.pvseg_start == expected_pvseg_offset:
                log.info(
                    'LV %s/%s segment at PV offset %d is where it should be.',
                    lvm_seg.vg_name,
                    lvm_seg.lv_name,
                    lvm_seg.pvseg_start,
                )
                expected_pvseg_offset += lvm_seg.seg_size_pe
                continue
            log.info(
                'LV %s/%s segment at PV offset %d should be at PV offset %d.',
                lvm_seg.vg_name,
                lvm_seg.lv_name,
                lvm_seg.pvseg_start,
                expected_pvseg_offset,
            )
            here = pvoffset2seg[expected_pvseg_offset]
            to_move = min(lvm_seg.seg_size_pe, here.seg_size_pe)
            if here.segtype == 'free':
                log.info('There is a free space at PV offset %d (size %d).', expected_pvseg_offset, here.seg_size_pe)
                log.info('Moving %d extents to their proper place.', to_move)
                # Move data to this free space.
                check_call([
                    'pvmove', '-b',
                    '--alloc', 'anywhere',
                    f'{pv_name}:{lvm_seg.pvseg_start}+{to_move}',
                    f'{pv_name}:{here.pvseg_start}+{to_move}',
                ])
            elif here.segtype == 'linear':
                log.info(
                    'There is another segment at PV offset %d of size %d (part of %s/%s).',
                    expected_pvseg_offset,
                    here.seg_size_pe,
                    here.vg_name,
                    here.lv_name,
                )
                log.info('Moving %d extents to some other place.', to_move)
                check_call([
                    'pvmove', '-b',
                    '--alloc', 'anywhere',
                    f'{pv_name}:{here.pvseg_start}+{to_move}',
                    pv_name,
                ])
            else:
                raise RuntimeError

            return True
        log.info('Done inspecting %s', vg_lv_name)

    log.info('Done inspecting all LVs')
    return False


def main():
    logging.basicConfig(level=logging.DEBUG)
    while defrag('/dev/vda3'):
        pass


if __name__ == '__main__':
    main()
