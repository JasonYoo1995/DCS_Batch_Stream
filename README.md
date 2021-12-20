- **사용 스택** : AWS EC2(Ubuntu), Node.js, Master-Slave 시스템을 지원하는 통신 라이브러리(Abraxas, GearmaNode)
- **개발 기간** : 2021년 4월 24일 ~ 6월 14일
- **시연 영상** : [https://youtu.be/Y6chGcn8K0E](https://youtu.be/Y6chGcn8K0E)
- **프로젝트 내용 요약**
    - Hadoop Map-Reduce의 핵심 메커니즘을 Javascript 언어로 Low Level 수준 구현
    - 통신을 처리해주는 오픈소스 라이브러리의 도움을 받은 것 외에 모든 알고리즘은 바닥부터 직접 구현
    - 분산 처리를 통한 성능 향상
    - Sum Job과 Count Job 간의 Job Chaining
    - 3가지 Fault Tolerance 기법 구현
- **제작 문서**
    
    [Fault Tolerant한 분산 병렬 처리 문서](https://www.notion.so/Fault-Tolerant-b6e89a5c48884e1e99e3e8680df82626)
    
- **어려웠던 점 및 해결 방법**
    - 문제1
        - 1) 하나의 Job을 여러 개의 Task로 나누어 처리한다.
        2) 각 Task들을 여러 개의 Node들에 분산 시켜 처리한다.
        3) 각 Node들이 처리되는 속도와 응답 시점이 서로 다르다.
        4) 'Raw Data 입력 → Sum 연산 → Count 연산'의 순서대로 처리해야 한다.
        어떠한 상황에서도 위 4가지 조건을 동시에 만족시키면서 Integrity를 보장하는 알고리즘을 고안해야 했습니다.
    - 해결1
        - Queuing 메커니즘으로 구현합니다.
        - Sum에 대한 Input Data를 담는 Queue와 Count에 대한 Input Data를 담는 Queue를 두어 waiting을 합니다.
    - 문제2
        - while문 안에서 비동기 통신 코드를 수행할 때, 해당 통신에 등록된 콜백 함수에 대한 trigger가 발생했음에도 불구하고, 이를 위한 context switch가 일어나지 않아 deadlock이 발생했습니다.
    - 해결2
        - while문 안에서 busy waiting 타입의 delay를 주면 CPU가 idle 상태로 바뀔 틈이 없어 context switching이 일어나지 않는다는 것이 원인임을 알아냈고, while문 안에서 sleep 타입의 delay를 줘서 CPU를 idle 상태로 만들어 context switching을 유도함으로써 해결하였습니다.
