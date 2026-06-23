// ============================================================
// Netlify Edge Function - Claude API 프록시
// 핵심: keep-alive 공백을 주기적으로 흘려보내 연결 유지
//      → Claude 분석이 오래 걸려도 ERR_CONNECTION_CLOSED 방지
// 브라우저는 응답 끝의 JSON만 파싱 (앞쪽 공백 무시)
// ============================================================

const FREE_LIMIT = 5;

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'Content-Type',
  'Access-Control-Allow-Methods': 'POST, OPTIONS',
};

async function redisCmd(command) {
  const url = Netlify.env.get('UPSTASH_REDIS_REST_URL');
  const token = Netlify.env.get('UPSTASH_REDIS_REST_TOKEN');
  if (!url || !token) throw new Error('Upstash 환경변수 없음');
  const res = await fetch(url, {
    method: 'POST',
    headers: { Authorization: `Bearer ${token}`, 'Content-Type': 'application/json' },
    body: JSON.stringify(command)
  });
  const data = await res.json();
  return data.result;
}
async function redisGet(key) { const r = await redisCmd(['GET', key]); return r ? JSON.parse(r) : null; }
async function redisSet(key, value) { await redisCmd(['SET', key, JSON.stringify(value)]); }
async function redisDel(key) { await redisCmd(['DEL', key]); }
async function redisKeys(pattern) { return (await redisCmd(['KEYS', pattern])) || []; }

export default async (request, context) => {
  if (request.method === 'OPTIONS') {
    return new Response('', { status: 200, headers: corsHeaders });
  }
  if (request.method !== 'POST') {
    return new Response(JSON.stringify({ error: '허용되지 않는 메서드' }), { status: 405, headers: corsHeaders });
  }

  const body = await request.json();

  // ── 관리자 조회 ──
  if (body.adminAction === 'getUsers') {
    if (body.adminPw !== Netlify.env.get('ADMIN_PW')) {
      return new Response(JSON.stringify({ error: '비밀번호 오류' }), { status: 403, headers: corsHeaders });
    }
    try {
      const keys = await redisKeys('user_*');
      const users = [];
      for (const key of keys) {
        const data = await redisGet(key);
        if (data) users.push({ userId: key.replace('user_', ''), ...data });
      }
      users.sort((a, b) => (b.lastUsed || '').localeCompare(a.lastUsed || ''));
      return new Response(JSON.stringify({ users }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
    } catch (e) {
      return new Response(JSON.stringify({ error: e.message }), { status: 500, headers: corsHeaders });
    }
  }

  // ── 관리자 리셋 ──
  if (body.adminAction === 'resetUser') {
    if (body.adminPw !== Netlify.env.get('ADMIN_PW')) {
      return new Response(JSON.stringify({ error: '비밀번호 오류' }), { status: 403, headers: corsHeaders });
    }
    await redisDel('user_' + body.targetUserId);
    return new Response(JSON.stringify({ ok: true }), { status: 200, headers: corsHeaders });
  }

  // ── 사용 횟수만 조회 (분석 실행 없이) ──
  if (body.checkUsage) {
    const uid = body.userId;
    if (!uid) return new Response(JSON.stringify({ error: '사용자 정보 없음' }), { status: 400, headers: corsHeaders });
    const ckey = 'user_' + uid.replace(/[^a-zA-Z0-9가-힣_]/g, '_');
    let u = { count: 0 };
    try { const s = await redisGet(ckey); if (s) u = s; } catch (e) {}
    return new Response(JSON.stringify({ _freeUsed: u.count, _freeRemain: FREE_LIMIT - u.count, _freeLimit: FREE_LIMIT }), { status: 200, headers: { ...corsHeaders, 'Content-Type': 'application/json' } });
  }

  // ── 일반 API 호출 ──
  const { userId, payload, countUsage, adminPw } = body;
  if (!userId || userId.length < 2) {
    return new Response(JSON.stringify({ error: '사용자 정보가 필요합니다.' }), { status: 400, headers: corsHeaders });
  }

  // ★ 관리자 무제한: 비밀번호가 맞으면 횟수 체크/차감 모두 건너뜀
  const isAdmin = adminPw && adminPw === Netlify.env.get('ADMIN_PW');

  const key = 'user_' + userId.replace(/[^a-zA-Z0-9가-힣_]/g, '_');

  let usageData = { count: 0, firstUsed: null, lastUsed: null, userName: userId };
  if (!isAdmin) {
    try {
      const saved = await redisGet(key);
      if (saved) usageData = saved;
    } catch (e) {
      return new Response(JSON.stringify({ error: 'Redis 연결 실패: ' + e.message }), { status: 500, headers: corsHeaders });
    }

    // 차감 대상(피해분석)일 때만 한도 체크
    if (countUsage && usageData.count >= FREE_LIMIT) {
      return new Response(JSON.stringify({ error: `무료 사용 ${FREE_LIMIT}건이 모두 소진되었습니다. (${userId}님)`, used: usageData.count, limit: FREE_LIMIT }), { status: 429, headers: corsHeaders });
    }
  }

  const apiKey = Netlify.env.get('ANTHROPIC_API_KEY');
  if (!apiKey) {
    return new Response(JSON.stringify({ error: 'ANTHROPIC_API_KEY 환경변수 없음' }), { status: 500, headers: corsHeaders });
  }

  // ★★★ keep-alive 스트림 ★★★
  const encoder = new TextEncoder();
  const stream = new ReadableStream({
    async start(controller) {
      let alive = true;
      const keepAlive = setInterval(() => {
        if (alive) {
          try { controller.enqueue(encoder.encode(' ')); } catch (e) {}
        }
      }, 1000);

      try {
        const apiRes = await fetch('https://api.anthropic.com/v1/messages', {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'x-api-key': apiKey, 'anthropic-version': '2023-06-01' },
          body: JSON.stringify(payload),
        });
        const apiData = await apiRes.json();

        alive = false;
        clearInterval(keepAlive);

        if (!apiRes.ok) {
          controller.enqueue(encoder.encode('\n' + JSON.stringify({ error: 'Claude API: ' + (apiData.error?.message || apiRes.status) })));
          controller.close();
          return;
        }

        // ★ 차감 대상(피해분석)이고 관리자가 아닐 때만 횟수 증가
        if (countUsage && !isAdmin) {
          usageData.count += 1;
          usageData.lastUsed = new Date().toISOString();
          if (!usageData.firstUsed) usageData.firstUsed = usageData.lastUsed;
          usageData.userName = userId;
          try { await redisSet(key, usageData); } catch (e) {}
        }

        const result = { ...apiData, _freeUsed: usageData.count, _freeRemain: FREE_LIMIT - usageData.count, _freeLimit: FREE_LIMIT, _isAdmin: isAdmin };
        controller.enqueue(encoder.encode('\n' + JSON.stringify(result)));
        controller.close();
      } catch (err) {
        alive = false;
        clearInterval(keepAlive);
        controller.enqueue(encoder.encode('\n' + JSON.stringify({ error: '서버 오류: ' + err.message })));
        controller.close();
      }
    }
  });

  return new Response(stream, {
    status: 200,
    headers: { ...corsHeaders, 'Content-Type': 'text/plain; charset=utf-8' }
  });
};

export const config = { path: '/api/claude' };
