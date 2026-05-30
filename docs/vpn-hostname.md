# ใช้ Hostname แทน VPN IP (เครื่องที่รัน proxy-port)

> ปัญหา: เครื่องที่รัน `proxy-port` อยู่บน **Microsoft VPN องค์กร** และ VPN IP เปลี่ยนบ่อย
> ทำให้เครื่องอื่น (clients) เรียกเข้ามาที่ `proxy-port-machine:port` ไม่ถูก
>
> เป้าหมาย: ให้ clients เรียกผ่าน **hostname คงที่** ที่ตาม IP ไปเองทุกครั้งที่ IP เปลี่ยน

```
[ clients ] ──▶ <hostname>:6379 / :26379 ──[ proxy-port ]──▶ 10.8.0.25 (redis บน VPN)
                     ▲ คงที่              ▲ VPN IP เปลี่ยนบ่อย
```

---

## หลักการ (อ่านก่อน)

Hostname จะ "เรียกถูก" ได้ **ก็ต่อเมื่อมี DNS ที่ map ชื่อ → IP ปัจจุบันให้อัตโนมัติ**
ตัวชื่อเองไม่มีเวทมนตร์ — ถ้าไม่มีระบบ resolve การเปลี่ยนจาก IP เป็นชื่อก็ไม่ช่วย

Microsoft VPN องค์กรส่วนใหญ่ใช้ **AD-integrated DNS + Dynamic DNS registration**:
เครื่องที่ **domain-joined** จะ register VPN IP ปัจจุบันเข้า DNS อัตโนมัติ → FQDN ตาม IP ไปเอง

| เครื่อง proxy-port | ได้ hostname อัตโนมัติไหม |
|---|---|
| Windows **domain-joined** | ✅ มักได้ฟรี — ไปที่ [ขั้นตอน A](#a-เครื่อง-domain-joined--ใช้-fqdn-ได้เลย) |
| macOS / ไม่ได้ domain-joined | ❌ มักไม่ auto-register — ไปที่ [ขั้นตอน B](#b-resolve-ชื่อไม่ได้--fallback) |

---

## A. เครื่อง domain-joined → ใช้ FQDN ได้เลย

### A1. หา FQDN ของเครื่อง proxy-port

```bash
# Windows
ipconfig /all                          # ดู "Host Name" + "Primary DNS Suffix"
echo %COMPUTERNAME%.%USERDNSDOMAIN%    # = FQDN เช่น myhost.corp.kbank.com

# macOS / Linux
hostname -f                            # FQDN
```

หรือ reverse-lookup จาก IP ปัจจุบัน (รันที่เครื่อง client ก็ได้):

```bash
nslookup <vpn-ip-ปัจจุบัน>
dig -x <vpn-ip> +short                 # คืน PTR = ชื่อของเครื่อง
```

### A2. ทดสอบว่าชื่อ "ตาม IP" จริง — สำคัญที่สุด

```bash
nslookup myhost.corp.kbank.com         # จด IP ที่ได้
# → ตัด/ต่อ VPN ใหม่ให้ IP เปลี่ยน
nslookup myhost.corp.kbank.com         # ถ้าได้ IP ใหม่ = Dynamic DNS ทำงาน ✅
```

> ⚠️ เครื่องมีหลาย interface (LAN + VPN) → ต้องเช็กว่า `nslookup` คืน **VPN IP**
> ที่ clients เข้าถึงได้จริง ไม่ใช่ LAN IP

### A3. ให้ clients เรียกผ่าน hostname

clients ชี้มาที่ `myhost.corp.kbank.com:6379` / `:26379` แทน IP — จบ
IP เปลี่ยนกลายเป็นมองไม่เห็น และ **เลิกใช้ `set-redis-vpn.sh` ได้** (สคริปต์นั้นแก้ฝั่ง redis remote ไม่เกี่ยวกับ listen)

---

## B. resolve ชื่อไม่ได้ → Fallback

ถ้า A2 ไม่คืน IP (เคสปกติของ macOS / non-domain) เลือกทางใดทางหนึ่ง:

### B1. ขอ IT ทำ DNS A-record / Static VPN IP (แนะนำสุด)

Always On VPN / Azure VPN กำหนด IP ต่อ device/user ได้ — ขอ **reservation**
แล้ว IP เลิกเปลี่ยน หรือขอ **A-record คงที่** ชี้มาที่เครื่อง → ปัญหาหายถาวร ไม่ต้องดูแลอะไรเพิ่ม

### B2. DDNS updater — push IP เองทุกครั้งที่เปลี่ยน

ถ้า corporate DNS อนุญาต dynamic update (`nsupdate` / RFC 2136) รันสคริปต์นี้บนเครื่อง proxy-port
(ตั้ง cron / launchd ทุก 1–2 นาที):

```bash
#!/usr/bin/env bash
# ddns-update.sh — register VPN IP ปัจจุบันเข้า corporate DNS
set -euo pipefail

FQDN="myhost.corp.kbank.com"
DNS_SERVER="10.x.x.x"          # corporate DNS ที่รับ dynamic update
ZONE="corp.kbank.com"
TTL=60

# ดึง IP ของ VPN interface (แก้ชื่อ interface ให้ตรงเครื่อง)
#   macOS: utun3 / ppp0   |   Linux: tun0   |   Windows: ใช้ PowerShell แทน
VPN_IP="$(ifconfig utun3 2>/dev/null | awk '/inet /{print $2}')"
[[ -z "$VPN_IP" ]] && { echo "no VPN IP on interface" >&2; exit 1; }

nsupdate <<EOF
server $DNS_SERVER
zone $ZONE
update delete $FQDN A
update add $FQDN $TTL A $VPN_IP
send
EOF

echo "registered $FQDN -> $VPN_IP"
```

ตั้งเวลา (macOS launchd หรือ cron):

```bash
# cron ทุก 2 นาที
*/2 * * * * /path/to/ddns-update.sh >> /tmp/ddns.log 2>&1
```

### B3. /etc/hosts ที่ฝั่ง clients (เฉพาะ POC)

ง่ายสุดแต่ต้องไล่แก้ทุกเครื่องเมื่อ IP เปลี่ยน — **ไม่ scale** ใช้ชั่วคราวเท่านั้น:

```
<vpn-ip>   myhost.corp.kbank.com
```

---

## สรุปการตัดสินใจ

| สถานการณ์ | ทำอะไร |
|---|---|
| Windows domain-joined + DDNS ทำงาน | ใช้ FQDN ที่ได้จาก A1, ทดสอบด้วย A2, จบ |
| ขอ IT ได้ | B1 — static IP / A-record คงที่ (ถาวร ไม่ต้องดูแล) |
| คุม DNS dynamic update เองได้ | B2 — สคริปต์ DDNS + cron |
| POC เร็วๆ ไม่กี่เครื่อง | B3 — /etc/hosts |
```

